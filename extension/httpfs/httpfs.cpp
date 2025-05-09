#include "httpfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/logging/http_logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "http_state.hpp"

#include <chrono>
#include <map>
#include <string>
#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

void HTTPFileSystem::InitializeHeaders(HTTPHeaders &header_map, const HTTPParams &http_params) {
	for (auto &entry : http_params.extra_headers) {
		header_map.Insert(entry.first, entry.second);
	}
}

duckdb::unique_ptr<duckdb_httplib_openssl::Headers> TransformHeaders(const HTTPHeaders &header_map) {
	auto headers = make_uniq<duckdb_httplib_openssl::Headers>();
	for(auto &entry : header_map) {
		headers->insert(entry);
	}
	return headers;
}

duckdb::unique_ptr<duckdb_httplib_openssl::Headers> HTTPFileSystem::TransformHeaders(const HTTPHeaders &header_map) {
	auto headers = make_uniq<duckdb_httplib_openssl::Headers>();
	for (auto &entry : header_map) {
		headers->insert(entry);
	}
	return headers;
}

HTTPParams HTTPParams::ReadFrom(optional_ptr<FileOpener> opener, optional_ptr<FileOpenerInfo> info) {
	auto result = HTTPParams();

	// No point in continueing without an opener
	if (!opener) {
		return result;
	}

	Value value;

	// Setting lookups
	FileOpener::TryGetCurrentSetting(opener, "http_timeout", result.timeout, info);
	FileOpener::TryGetCurrentSetting(opener, "force_download", result.force_download, info);
	FileOpener::TryGetCurrentSetting(opener, "http_retries", result.retries, info);
	FileOpener::TryGetCurrentSetting(opener, "http_retry_wait_ms", result.retry_wait_ms, info);
	FileOpener::TryGetCurrentSetting(opener, "http_retry_backoff", result.retry_backoff, info);
	FileOpener::TryGetCurrentSetting(opener, "http_keep_alive", result.keep_alive, info);
	FileOpener::TryGetCurrentSetting(opener, "enable_server_cert_verification", result.enable_server_cert_verification,
	                                 info);
	FileOpener::TryGetCurrentSetting(opener, "ca_cert_file", result.ca_cert_file, info);
	FileOpener::TryGetCurrentSetting(opener, "hf_max_per_page", result.hf_max_per_page, info);

	// HTTP Secret lookups
	KeyValueSecretReader settings_reader(*opener, info, "http");

	string proxy_setting;
	if (settings_reader.TryGetSecretKeyOrSetting<string>("http_proxy", "http_proxy", proxy_setting) &&
	    !proxy_setting.empty()) {
		idx_t port;
		string host;
		HTTPUtil::ParseHTTPProxyHost(proxy_setting, host, port);
		result.http_proxy = host;
		result.http_proxy_port = port;
	}
	settings_reader.TryGetSecretKeyOrSetting<string>("http_proxy_username", "http_proxy_username",
	                                                 result.http_proxy_username);
	settings_reader.TryGetSecretKeyOrSetting<string>("http_proxy_password", "http_proxy_password",
	                                                 result.http_proxy_password);
	settings_reader.TryGetSecretKey<string>("bearer_token", result.bearer_token);

	Value extra_headers;
	if (settings_reader.TryGetSecretKey("extra_http_headers", extra_headers)) {
		auto children = MapValue::GetChildren(extra_headers);
		for (const auto &child : children) {
			auto kv = StructValue::GetChildren(child);
			D_ASSERT(kv.size() == 2);
			result.extra_headers[kv[0].GetValue<string>()] = kv[1].GetValue<string>();
		}
	}

	return result;
}

unique_ptr<HTTPClient> HTTPClientCache::GetClient() {
	lock_guard<mutex> lck(lock);
	if (clients.size() == 0) {
		return nullptr;
	}

	auto client = std::move(clients.back());
	clients.pop_back();
	return client;
}

void HTTPClientCache::StoreClient(unique_ptr<HTTPClient> client) {
	lock_guard<mutex> lck(lock);
	clients.push_back(std::move(client));
}

void HTTPFileSystem::ParseUrl(string &url, string &path_out, string &proto_host_port_out) {
	if (url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) {
		throw IOException("URL needs to start with http:// or https://");
	}
	auto slash_pos = url.find('/', 8);
	if (slash_pos == string::npos) {
		throw IOException("URL needs to contain a '/' after the host");
	}
	proto_host_port_out = url.substr(0, slash_pos);

	path_out = url.substr(slash_pos);

	if (path_out.empty()) {
		throw IOException("URL needs to contain a path");
	}
}

// FIXME: this is copied form http_util.cpp
unique_ptr<HTTPResponse> TransformResponse(const duckdb_httplib_openssl::Response &response) {
	auto status_code = HTTPUtil::ToStatusCode(response.status);
	auto result = make_uniq<HTTPResponse>(status_code);
	result->body = response.body;
	result->reason = response.reason;
	for (auto &entry : response.headers) {
		result->headers.Insert(entry.first, entry.second);
	}
	return result;
}

unique_ptr<HTTPResponse> HTTPFileSystem::TransformResult(duckdb_httplib_openssl::Result &&res) {
	if (res.error() == duckdb_httplib_openssl::Error::Success) {
		auto &response = res.value();
		return TransformResponse(response);
	} else {
		auto result = make_uniq<HTTPResponse>(HTTPStatusCode::INVALID);
		result->request_error = to_string(res.error());
		return result;
	}
}

// Retry the request performed by fun using the exponential backoff strategy defined in params. Before retry, the
// retry callback is called
duckdb::unique_ptr<HTTPResponse>
HTTPFileSystem::RunRequestWithRetry(const std::function<unique_ptr<HTTPResponse>(void)> &request, string &url,
                                    string method, const HTTPParams &params,
                                    const std::function<void(void)> &retry_cb) {
	idx_t tries = 0;
	while (true) {
		std::exception_ptr caught_e = nullptr;
		unique_ptr<HTTPResponse> response;

		try {
			response = request();
		} catch (IOException &e) {
			caught_e = std::current_exception();
		} catch (HTTPException &e) {
			caught_e = std::current_exception();
		}

		// Note: all duckdb_httplib_openssl::Error types will be retried.
		bool should_retry = !response || response->ShouldRetry();
		if (!should_retry) {
			return response;
		}

		tries += 1;
		if (tries <= params.retries) {
			if (tries > 1) {
				uint64_t sleep_amount = (uint64_t)((float)params.retry_wait_ms * pow(params.retry_backoff, tries - 2));
				std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
			}
			if (retry_cb) {
				retry_cb();
			}
		} else {
			if (caught_e) {
				std::rethrow_exception(caught_e);
			} else if (response && !response->HasRequestError()) {
				throw HTTPException(*response, "Request returned HTTP %d for HTTP %s to '%s'", static_cast<int>(response->status), method, url);
			} else {
				string error = response ? response->GetError() : "Unknown error";
				throw IOException("%s error for HTTP %s to '%s'", error, method, url);
			}
		}
	}
}

unique_ptr<HTTPResponse> HTTPFileSystem::PostRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                        duckdb::unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
                                                        char *buffer_in, idx_t buffer_in_len, string params) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	InitializeHeaders(header_map, hfh.http_params);
	std::function<unique_ptr<HTTPResponse>(void)> request([&]() {
		auto client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh);

		PostRequestInfo post_request(path, header_map, const_data_ptr_cast(buffer_in), buffer_in_len, hfh.state.get());
		auto result = client->Post(post_request);

		buffer_out = std::move(post_request.buffer_out);
		buffer_out_len = post_request.buffer_out_len;
		return result;
	});
	return RunRequestWithRetry(request, url, "POST", hfh.http_params);
}

class HTTPLibClient : public HTTPClient {
public:
	HTTPLibClient(const HTTPParams &http_params, const char *proto_host_port, optional_ptr<HTTPLogger> logger) {
		client = make_uniq<duckdb_httplib_openssl::Client>(proto_host_port);
		client->set_follow_location(true);
		client->set_keep_alive(http_params.keep_alive);
		if (!http_params.ca_cert_file.empty()) {
			client->set_ca_cert_path(http_params.ca_cert_file.c_str());
		}
		client->enable_server_certificate_verification(http_params.enable_server_cert_verification);
		client->set_write_timeout(http_params.timeout, http_params.timeout_usec);
		client->set_read_timeout(http_params.timeout, http_params.timeout_usec);
		client->set_connection_timeout(http_params.timeout, http_params.timeout_usec);
		client->set_decompress(false);
		if (logger) {
			SetLogger(*logger);
		}
		if (!http_params.bearer_token.empty()) {
			client->set_bearer_token_auth(http_params.bearer_token.c_str());
		}

		if (!http_params.http_proxy.empty()) {
			client->set_proxy(http_params.http_proxy, http_params.http_proxy_port);

			if (!http_params.http_proxy_username.empty()) {
				client->set_proxy_basic_auth(http_params.http_proxy_username, http_params.http_proxy_password);
			}
		}
	}

	void SetLogger(HTTPLogger &logger) override {
		client->set_logger(
			logger.GetLogger<duckdb_httplib_openssl::Request, duckdb_httplib_openssl::Response>());
	}
	duckdb_httplib_openssl::Client &GetHTTPLibClient() override {
		return *client;
	}
	unique_ptr<HTTPResponse> Get(GetRequestInfo &info) override {
		if (info.state) {
			info.state->get_count++;
		}
        auto headers = TransformHeaders(info.headers);
        return HTTPFileSystem::TransformResult(client->Get(info.path.c_str(), *headers,
		    [&](const duckdb_httplib_openssl::Response &response) {
		    	auto http_response = TransformResponse(response);
		    	return info.response_handler(*http_response);
		    },
            [&](const char *data, size_t data_length) {
				return info.content_handler(const_data_ptr_cast(data), data_length);
            }));
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &info) override {
        if (info.state) {
            info.state->put_count++;
            info.state->total_bytes_sent += info.buffer_in_len;
        }
        auto headers = TransformHeaders(info.headers);
        return HTTPFileSystem::TransformResult(client->Put(info.path.c_str(), *headers, const_char_ptr_cast(info.buffer_in), info.buffer_in_len, info.content_type));
	}

	unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) override {
        if (info.state) {
            info.state->head_count++;
        }
        auto headers = TransformHeaders(info.headers);
        return HTTPFileSystem::TransformResult(client->Head(info.path.c_str(), *headers));
	}

	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &info) override {
        if (info.state) {
            info.state->delete_count++;
        }
        auto headers = TransformHeaders(info.headers);
        return HTTPFileSystem::TransformResult(client->Delete(info.path.c_str(), *headers));
	}

	unique_ptr<HTTPResponse> Post(PostRequestInfo &info) override {
        if (info.state) {
            info.state->post_count++;
            info.state->total_bytes_sent += info.buffer_in_len;
        }
		idx_t out_offset = 0;
        // We use a custom Request method here, because there is no Post call with a contentreceiver in httplib
        duckdb_httplib_openssl::Request req;
        req.method = "POST";
        req.path = info.path;
        req.headers = *TransformHeaders(info.headers);
        req.headers.emplace("Content-Type", "application/octet-stream");
        req.content_receiver = [&](const char *data, size_t data_length, uint64_t /*offset*/,
                                   uint64_t /*total_length*/) {
            if (info.state) {
                info.state->total_bytes_received += data_length;
            }
            if (out_offset + data_length > info.buffer_out_len) {
                // Buffer too small, increase its size by at least 2x to fit the new value
                auto new_size = MaxValue<idx_t>(out_offset + data_length, info.buffer_out_len * 2);
                auto tmp = duckdb::unique_ptr<char[]> {new char[new_size]};
                memcpy(tmp.get(), info.buffer_out.get(), info.buffer_out_len);
                info.buffer_out = std::move(tmp);
                info.buffer_out_len = new_size;
            }
            memcpy(info.buffer_out.get() + out_offset, data, data_length);
            out_offset += data_length;
            return true;
        };
        req.body.assign(const_char_ptr_cast(info.buffer_in), info.buffer_in_len);
        return HTTPFileSystem::TransformResult(client->send(req));
	}

	unique_ptr<duckdb_httplib_openssl::Client> client;
};

unique_ptr<HTTPClient> HTTPFileSystem::GetClient(const HTTPParams &http_params,
                                                                     const char *proto_host_port,
                                                                     optional_ptr<HTTPFileHandle> hfh) {
	optional_ptr<HTTPLogger> logger;
	if (hfh) {
		logger = hfh->http_logger.get();
	}
	auto client = make_uniq<HTTPLibClient>(http_params, proto_host_port, logger);
	return client;
}

unique_ptr<HTTPResponse> HTTPFileSystem::PutRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                       char *buffer_in, idx_t buffer_in_len, string params) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	InitializeHeaders(header_map, hfh.http_params);

	std::function<unique_ptr<HTTPResponse>(void)> request([&]() {
		auto client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh);

		PutRequestInfo put_request(path, header_map, (const_data_ptr_t) buffer_in, buffer_in_len, "application/octet-stream", hfh.state.get());
		return client->Put(put_request);
	});

	return RunRequestWithRetry(request, url, "PUT", hfh.http_params);
}

unique_ptr<HTTPResponse> HTTPFileSystem::HeadRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	InitializeHeaders(header_map, hfh.http_params);
	auto http_client = hfh.GetClient(nullptr);

	std::function<unique_ptr<HTTPResponse>(void)> request([&]() {
		HeadRequestInfo head_request(path, header_map, hfh.state.get());
		return http_client->Head(head_request);
	});

	// Refresh the client on retries
	std::function<void(void)> on_retry(
	    [&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "HEAD", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}
unique_ptr<HTTPResponse> HTTPFileSystem::DeleteRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	InitializeHeaders(header_map, hfh.http_params);
	auto http_client = hfh.GetClient(nullptr);

	std::function<unique_ptr<HTTPResponse>(void)> request([&]() {
		DeleteRequestInfo delete_request(path, header_map, hfh.state.get());
		return http_client->Delete(delete_request);
	});

	// Refresh the client on retries
	std::function<void(void)> on_retry(
		[&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "DELETE", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}

unique_ptr<HTTPResponse> HTTPFileSystem::GetRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	InitializeHeaders(header_map, hfh.http_params);

	D_ASSERT(hfh.cached_file_handle);

	auto http_client = hfh.GetClient(nullptr);

	std::function<unique_ptr<HTTPResponse>(void)> request([&]() {
		GetRequestInfo get_request(path, header_map,
		    [&](const HTTPResponse &response) {
			    if (static_cast<int>(response.status) >= 400) {
				    string error = "HTTP GET error on '" + url + "' (HTTP " + to_string(static_cast<int>(response.status)) + ")";
				    if (response.status == HTTPStatusCode::RangeNotSatisfiable_416) {
					    error += " This could mean the file was changed. Try disabling the duckdb http metadata cache "
					             "if enabled, and confirm the server supports range requests.";
				    }
				    throw IOException(error);
			    }
			    return true;
		    },
		    [&](const_data_ptr_t data, idx_t data_length) {
			    D_ASSERT(hfh.state);
			    if (hfh.state) {
				    hfh.state->total_bytes_received += data_length;
			    }
			    if (!hfh.cached_file_handle->GetCapacity()) {
				    hfh.cached_file_handle->AllocateBuffer(data_length);
				    hfh.length = data_length;
				    hfh.cached_file_handle->Write(const_char_ptr_cast(data), data_length);
			    } else {
				    auto new_capacity = hfh.cached_file_handle->GetCapacity();
				    while (new_capacity < hfh.length + data_length) {
					    new_capacity *= 2;
				    }
				    // Grow buffer when running out of space
				    if (new_capacity != hfh.cached_file_handle->GetCapacity()) {
					    hfh.cached_file_handle->GrowBuffer(new_capacity, hfh.length);
				    }
				    // We can just copy stuff
				    hfh.cached_file_handle->Write(const_char_ptr_cast(data), data_length, hfh.length);
				    hfh.length += data_length;
			    }
			    return true;
		    }, hfh.state.get());
		return http_client->Get(get_request);
	});

	std::function<void(void)> on_retry(
	    [&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "GET", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}

unique_ptr<HTTPResponse> HTTPFileSystem::GetRangeRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                            idx_t file_offset, char *buffer_out, idx_t buffer_out_len) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	string path, proto_host_port;
	ParseUrl(url, path, proto_host_port);
	InitializeHeaders(header_map, hfh.http_params);
	auto headers = TransformHeaders(header_map);

	// send the Range header to read only subset of file
	string range_expr = "bytes=" + to_string(file_offset) + "-" + to_string(file_offset + buffer_out_len - 1);
	headers->insert(pair<string, string>("Range", range_expr));

	auto http_client = hfh.GetClient(nullptr);

	idx_t out_offset = 0;

	std::function<unique_ptr<HTTPResponse>(void)> request([&]() {
		if (hfh.state) {
			hfh.state->get_count++;
		}
		auto &httplib_client = http_client->GetHTTPLibClient();
		return TransformResult(httplib_client.Get(
		    path.c_str(), *headers,
		    [&](const duckdb_httplib_openssl::Response &response) {
			    if (response.status >= 400) {
				    string error = "HTTP GET error on '" + url + "' (HTTP " + to_string(response.status) + ")";
				    if (response.status == 416) {
					    error += " This could mean the file was changed. Try disabling the duckdb http metadata cache "
					             "if enabled, and confirm the server supports range requests.";
				    }
				    throw HTTPException(response, error);
			    }
			    if (response.status < 300) { // done redirecting
				    out_offset = 0;
				    if (response.has_header("Content-Length")) {
					    auto content_length = stoll(response.get_header_value("Content-Length", 0));
					    if ((idx_t)content_length != buffer_out_len) {
						    throw IOException("HTTP GET error: Content-Length from server mismatches requested "
						                      "range, server may not support range requests.");
					    }
				    }
			    }
			    return true;
		    },
		    [&](const char *data, size_t data_length) {
			    if (hfh.state) {
				    hfh.state->total_bytes_received += data_length;
			    }
			    if (buffer_out != nullptr) {
				    if (data_length + out_offset > buffer_out_len) {
					    // As of v0.8.2-dev4424 we might end up here when very big files are served from servers
					    // that returns more data than requested via range header. This is an uncommon but legal
					    // behaviour, so we have to improve logic elsewhere to properly handle this case.

					    // To avoid corruption of memory, we bail out.
					    throw IOException("Server sent back more data than expected, `SET force_download=true` might "
					                      "help in this case");
				    }
				    memcpy(buffer_out + out_offset, data, data_length);
				    out_offset += data_length;
			    }
			    return true;
		    }));
	});

	std::function<void(void)> on_retry(
	    [&]() { http_client = GetClient(hfh.http_params, proto_host_port.c_str(), &hfh); });

	auto response = RunRequestWithRetry(request, url, "GET Range", hfh.http_params, on_retry);
	hfh.StoreClient(std::move(http_client));
	return response;
}

void TimestampToTimeT(timestamp_t timestamp, time_t &result) {
	auto components = Timestamp::GetComponents(timestamp);
	struct tm tm {};
	tm.tm_year = components.year - 1900;
	tm.tm_mon = components.month - 1;
	tm.tm_mday = components.day;
	tm.tm_hour = components.hour;
	tm.tm_min = components.minute;
	tm.tm_sec = components.second;
	tm.tm_isdst = 0;
	result = mktime(&tm);
}

HTTPFileHandle::HTTPFileHandle(FileSystem &fs, const OpenFileInfo &file, FileOpenFlags flags, const HTTPParams &http_params)
    : FileHandle(fs, file.path, flags), http_params(http_params), flags(flags), length(0), buffer_available(0),
      buffer_idx(0), file_offset(0), buffer_start(0), buffer_end(0) {
	// check if the handle has extended properties that can be set directly in the handle
	// if we have these properties we don't need to do a head request to obtain them later
	if (file.extended_info) {
		auto &info = file.extended_info->options;
		auto lm_entry = info.find("last_modified");
		if (lm_entry != info.end()) {
			TimestampToTimeT(lm_entry->second.GetValue<timestamp_t>(), last_modified);
		}
		auto etag_entry = info.find("etag");
		if (etag_entry != info.end()) {
			etag = StringValue::Get(etag_entry->second);
		}
		auto fs_entry = info.find("file_size");
		if (fs_entry != info.end()) {
			length = fs_entry->second.GetValue<uint64_t>();
		}
		if (lm_entry != info.end() && etag_entry != info.end() && fs_entry != info.end()) {
			// we found all relevant entries (last_modified, etag and file size)
			// skip head request
			initialized = true;
		}
	}
}

unique_ptr<HTTPFileHandle> HTTPFileSystem::CreateHandle(const OpenFileInfo &file, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	FileOpenerInfo info;
	info.file_path = file.path;
	auto params = HTTPParams::ReadFrom(opener, info);

	auto secret_manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	if (secret_manager && transaction) {
		auto secret_match = secret_manager->LookupSecret(*transaction, file.path, "bearer");

		if (secret_match.HasMatch()) {
			const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);
			params.bearer_token = kv_secret.TryGetValue("token", true).ToString();
		}
	}

	auto result = duckdb::make_uniq<HTTPFileHandle>(*this, file, flags, params);
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context && ClientConfig::GetConfig(*client_context).enable_http_logging) {
		result->http_logger = client_context->client_data->http_logger.get();
	}
	return result;
}

unique_ptr<FileHandle> HTTPFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	if (flags.ReturnNullIfNotExists()) {
		try {
			auto handle = CreateHandle(file, flags, opener);
			handle->Initialize(opener);
			return std::move(handle);
		} catch (...) {
			return nullptr;
		}
	}

	auto handle = CreateHandle(file, flags, opener);
	handle->Initialize(opener);
	return std::move(handle);
}

// Buffered read from http file.
// Note that buffering is disabled when FileFlags::FILE_FLAGS_DIRECT_IO is set
void HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = handle.Cast<HTTPFileHandle>();

	D_ASSERT(hfh.state);
	if (hfh.cached_file_handle) {
		if (!hfh.cached_file_handle->Initialized()) {
			throw InternalException("Cached file not initialized properly");
		}
		memcpy(buffer, hfh.cached_file_handle->GetData() + location, nr_bytes);
		return;
	}

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set or when we are doing parallel reads
	bool skip_buffer = hfh.flags.DirectIO() || hfh.flags.RequireParallelAccess();
	if (skip_buffer && to_read > 0) {
		GetRangeRequest(hfh, hfh.path, {}, location, (char *)buffer, to_read);

		// Update handle status within critical section for parallel access.
		if (hfh.flags.RequireParallelAccess()) {
			std::lock_guard<std::mutex> lck(hfh.mu);
			hfh.buffer_available = 0;
			hfh.buffer_idx = 0;
			return;
		}

		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
	}

	idx_t start_offset = location;  // Start file offset to read from.
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			start_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(hfh.READ_BUFFER_LEN, hfh.length - start_offset);

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				GetRangeRequest(hfh, hfh.path, {}, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				start_offset += to_read;
				break;
			} else {
				GetRangeRequest(hfh, hfh.path, {}, start_offset, (char *)hfh.read_buffer.get(),
				                new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = start_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
}

int64_t HTTPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	hfh.file_offset += nr_bytes;
	return nr_bytes;
}

void HTTPFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("Writing to HTTP files not implemented");
}

int64_t HTTPFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	Write(handle, buffer, nr_bytes, hfh.file_offset);
	return nr_bytes;
}

void HTTPFileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("FileSync for HTTP files not implemented");
}

int64_t HTTPFileSystem::GetFileSize(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.length;
}

time_t HTTPFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.last_modified;
}

string HTTPFileSystem::GetVersionTag(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.etag;
}

bool HTTPFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	try {
		auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_READ, opener);
		auto &sfh = handle->Cast<HTTPFileHandle>();
		if (sfh.length == 0) {
			return false;
		}
		return true;
	} catch (...) {
		return false;
	};
}

bool HTTPFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("https://", 0) == 0 || fpath.rfind("http://", 0) == 0;
}

void HTTPFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	sfh.file_offset = location;
}

idx_t HTTPFileSystem::SeekPosition(FileHandle &handle) {
	auto &sfh = handle.Cast<HTTPFileHandle>();
	return sfh.file_offset;
}

optional_ptr<HTTPMetadataCache> HTTPFileSystem::GetGlobalCache() {
	lock_guard<mutex> lock(global_cache_lock);
	if (!global_metadata_cache) {
		global_metadata_cache = make_uniq<HTTPMetadataCache>(false, true);
	}
	return global_metadata_cache.get();
}

// Get either the local, global, or no cache depending on settings
static optional_ptr<HTTPMetadataCache> TryGetMetadataCache(optional_ptr<FileOpener> opener, HTTPFileSystem &httpfs) {
	auto db = FileOpener::TryGetDatabase(opener);
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!db) {
		return nullptr;
	}

	bool use_shared_cache = db->config.options.http_metadata_cache_enable;
	if (use_shared_cache) {
		return httpfs.GetGlobalCache();
	} else if (client_context) {
		return client_context->registered_state->GetOrCreate<HTTPMetadataCache>("http_cache", true, true).get();
	}
	return nullptr;
}

void HTTPFileHandle::FullDownload(HTTPFileSystem &hfs, bool &should_write_cache) {
	// We are going to download the file at full, we don't need to do no head request.
	const auto &cache_entry = state->GetCachedFile(path);
	cached_file_handle = cache_entry->GetHandle();
	if (!cached_file_handle->Initialized()) {
		// Try to fully download the file first
		const auto full_download_result = hfs.GetRequest(*this, path, {});
		if (full_download_result->status != HTTPStatusCode::OK_200) {
			throw HTTPException(*full_download_result, "Full download failed to to URL \"%s\": %s (%s)",
			                    full_download_result->url, static_cast<int>(full_download_result->status),
			                    full_download_result->GetError());
		}
		// Mark the file as initialized, set its final length, and unlock it to allowing parallel reads
		cached_file_handle->SetInitialized(length);
		// We shouldn't write these to cache
		should_write_cache = false;
	} else {
		length = cached_file_handle->GetSize();
	}
}

bool HTTPFileSystem::TryParseLastModifiedTime(const string &timestamp, time_t &result) {
	StrpTimeFormat::ParseResult parse_result;
	if (!StrpTimeFormat::TryParse("%a, %d %h %Y %T %Z", timestamp, parse_result)) {
		return false;
	}
	struct tm tm {};
	tm.tm_year = parse_result.data[0] - 1900;
	tm.tm_mon = parse_result.data[1] - 1;
	tm.tm_mday = parse_result.data[2];
	tm.tm_hour = parse_result.data[3];
	tm.tm_min = parse_result.data[4];
	tm.tm_sec = parse_result.data[5];
	tm.tm_isdst = 0;
	result = mktime(&tm);
	return true;
}

void HTTPFileHandle::LoadFileInfo() {
	if (initialized) {
		// already initialized
		return;
	}
	auto &hfs = file_system.Cast<HTTPFileSystem>();
	auto res = hfs.HeadRequest(*this, path, {});
	string range_length;

	if (res->status != HTTPStatusCode::OK_200) {
		if (flags.OpenForWriting() && res->status == HTTPStatusCode::NotFound_404) {
			if (!flags.CreateFileIfNotExists() && !flags.OverwriteExistingFile()) {
				throw IOException("Unable to open URL \"" + path +
				                  "\" for writing: file does not exist and CREATE flag is not set");
			}
			length = 0;
			return;
		} else {
			// HEAD request fail, use Range request for another try (read only one byte)
			if (flags.OpenForReading() && res->status != HTTPStatusCode::NotFound_404) {
				auto range_res = hfs.GetRangeRequest(*this, path, {}, 0, nullptr, 2);
				if (range_res->status != HTTPStatusCode::PartialContent_206 && range_res->status != HTTPStatusCode::Accepted_202 && range_res->status != HTTPStatusCode::OK_200) {
					throw IOException("Unable to connect to URL \"%s\": %d (%s).", path, static_cast<int>(res->status), res->GetError());
				}
				string content_range;
				if (range_res->headers.HasHeader("Content-Range")) {
					content_range = range_res->headers.GetHeaderValue("Content-Range");
				}
				auto range_find = content_range.find("/");
				if (!(range_find == std::string::npos || content_range.size() < range_find + 1)) {
					range_length = content_range.substr(range_find + 1);
					if (range_length != "*") {
						res = std::move(range_res);
					}
				}
			} else {
				// It failed again
				throw HTTPException(*res, "Unable to connect to URL \"%s\": %d (%s).", res->url,
				                    static_cast<int>(res->status), res->GetError());
			}
		}
	}
	if (res->headers.HasHeader("Last-Modified")) {
		HTTPFileSystem::TryParseLastModifiedTime(res->headers.GetHeaderValue("Last-Modified"), last_modified);
	}
	if (res->headers.HasHeader("Etag")) {
		etag = res->headers.GetHeaderValue("Etag");
	}
	initialized = true;
}

void HTTPFileHandle::Initialize(optional_ptr<FileOpener> opener) {
	auto &hfs = file_system.Cast<HTTPFileSystem>();
	state = HTTPState::TryGetState(opener);
	if (!state) {
		state = make_shared_ptr<HTTPState>();
	}

	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context && ClientConfig::GetConfig(*client_context).enable_http_logging) {
		http_logger = client_context->client_data->http_logger.get();
	}

	auto current_cache = TryGetMetadataCache(opener, hfs);

	bool should_write_cache = false;
	if (http_params.force_download) {
		FullDownload(hfs, should_write_cache);
		return;
	}

	if (current_cache && !flags.OpenForWriting()) {
		HTTPMetadataCacheEntry value;
		bool found = current_cache->Find(path, value);

		if (found) {
			last_modified = value.last_modified;
			length = value.length;
			etag = value.etag;

			if (flags.OpenForReading()) {
				read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
			}
			return;
		}

		should_write_cache = true;
	}

	// If we're writing to a file, we might as well remove it from the cache
	if (current_cache && flags.OpenForWriting()) {
		current_cache->Erase(path);
	}
	LoadFileInfo();

	// Initialize the read buffer now that we know the file exists
	if (flags.OpenForReading()) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[READ_BUFFER_LEN]);
	}

	if (state && length == 0) {
		FullDownload(hfs, should_write_cache);
	}
	if (should_write_cache) {
		current_cache->Insert(path, {length, last_modified, etag});
	}
}

unique_ptr<HTTPClient> HTTPFileHandle::GetClient(optional_ptr<ClientContext> context) {
	// Try to fetch a cached client
	auto cached_client = client_cache.GetClient();
	if (cached_client) {
		return cached_client;
	}

	// Create a new client
	return CreateClient(context);
}

unique_ptr<HTTPClient> HTTPFileHandle::CreateClient(optional_ptr<ClientContext> context) {
	// Create a new client
	string path_out, proto_host_port;
	HTTPFileSystem::ParseUrl(path, path_out, proto_host_port);
	auto http_client = HTTPFileSystem::GetClient(this->http_params, proto_host_port.c_str(), this);
	if (context && ClientConfig::GetConfig(*context).enable_http_logging) {
		http_logger = context->client_data->http_logger.get();
		http_client->SetLogger(*http_logger);
	}
	return http_client;
}

void HTTPFileHandle::StoreClient(unique_ptr<HTTPClient> client) {
	client_cache.StoreClient(std::move(client));
}

HTTPFileHandle::~HTTPFileHandle() = default;
} // namespace duckdb
