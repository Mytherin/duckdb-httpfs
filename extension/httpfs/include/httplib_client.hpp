#include "duckdb/common/http_util.hpp"

namespace duckdb {
class HTTPLogger;
class HTTPState;
class FileOpener;
struct FileOpenerInfo;

struct HTTPParams {
	virtual ~HTTPParams() = default;

	static constexpr uint64_t DEFAULT_TIMEOUT_SECONDS = 30; // 30 sec
	static constexpr uint64_t DEFAULT_RETRIES = 3;
	static constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
	static constexpr float DEFAULT_RETRY_BACKOFF = 4;

	uint64_t timeout = DEFAULT_TIMEOUT_SECONDS; // seconds component of a timeout
	uint64_t timeout_usec = 0;                  // usec component of a timeout
	uint64_t retries = DEFAULT_RETRIES;
	uint64_t retry_wait_ms = DEFAULT_RETRY_WAIT_MS;
	float retry_backoff = DEFAULT_RETRY_BACKOFF;

	string http_proxy;
	idx_t http_proxy_port;
	string http_proxy_username;
	string http_proxy_password;
	unordered_map<string, string> extra_headers;

public:
	void Initialize(DatabaseInstance &db);

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct HTTPFSParams : public HTTPParams {
	static constexpr bool DEFAULT_ENABLE_SERVER_CERT_VERIFICATION = false;
	static constexpr uint64_t DEFAULT_HF_MAX_PER_PAGE = 0;
	static constexpr bool DEFAULT_FORCE_DOWNLOAD = false;
	static constexpr bool DEFAULT_KEEP_ALIVE = true;

	bool force_download = DEFAULT_FORCE_DOWNLOAD;
	bool keep_alive = DEFAULT_KEEP_ALIVE;
	bool enable_server_cert_verification = DEFAULT_ENABLE_SERVER_CERT_VERIFICATION;
	idx_t hf_max_per_page = DEFAULT_HF_MAX_PER_PAGE;
	string ca_cert_file;
	string bearer_token;

	static HTTPFSParams ReadFrom(optional_ptr<FileOpener> opener, optional_ptr<FileOpenerInfo> info);
};

struct GetRequestInfo {
	GetRequestInfo(const string &path, const HTTPHeaders &headers,
	std::function<bool(const HTTPResponse &response)> response_handler, std::function<bool(const_data_ptr_t data, idx_t data_length)> content_handler, optional_ptr<HTTPState> state) :
		path(path), headers(headers), content_handler(content_handler), response_handler(response_handler), state(state) {}

	const string &path;
	const HTTPHeaders &headers;
	std::function<bool(const_data_ptr_t data, idx_t data_length)> content_handler;
	std::function<bool(const HTTPResponse &response)> response_handler;
	optional_ptr<HTTPState> state;
};

struct PutRequestInfo {
	PutRequestInfo(const string &path, const HTTPHeaders &headers, const_data_ptr_t buffer_in, idx_t buffer_in_len, const string &content_type, optional_ptr<HTTPState> state) :
		path(path), headers(headers), buffer_in(buffer_in), buffer_in_len(buffer_in_len), content_type(content_type), state(state) {}

	const string &path;
	const HTTPHeaders &headers;
	const_data_ptr_t buffer_in;
	idx_t buffer_in_len;
	const string &content_type;
	optional_ptr<HTTPState> state;
};

struct HeadRequestInfo {
	HeadRequestInfo(const string &path, const HTTPHeaders &headers, optional_ptr<HTTPState> state) :
		path(path), headers(headers), state(state) {}

	const string &path;
	const HTTPHeaders &headers;
	optional_ptr<HTTPState> state;
};

struct DeleteRequestInfo {
	DeleteRequestInfo(const string &path, const HTTPHeaders &headers, optional_ptr<HTTPState> state) :
		path(path), headers(headers), state(state) {}

	const string &path;
	const HTTPHeaders &headers;
	optional_ptr<HTTPState> state;
};

struct PostRequestInfo {
	PostRequestInfo(const string &path, const HTTPHeaders &headers, const_data_ptr_t buffer_in, idx_t buffer_in_len, optional_ptr<HTTPState> state) :
		path(path), headers(headers), buffer_in(buffer_in), buffer_in_len(buffer_in_len), state(state) {}

	const string &path;
	const HTTPHeaders &headers;
	const_data_ptr_t buffer_in;
	idx_t buffer_in_len;
	optional_ptr<HTTPState> state;
	duckdb::unique_ptr<char[]> buffer_out;
	idx_t buffer_out_len = 0;
};

class HTTPClient {
public:
	virtual ~HTTPClient() = default;

	virtual void SetLogger(HTTPLogger &logger) = 0;

	virtual unique_ptr<HTTPResponse> Get(GetRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Put(PutRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &info) = 0;
	virtual unique_ptr<HTTPResponse> Post(PostRequestInfo &info) = 0;
};

class HTTPFSUtil {
public:
	static unique_ptr<HTTPClient> InitializeClient(const HTTPParams &http_params,
											const char *proto_host_port,
											optional_ptr<HTTPLogger> logger);

	static unordered_map<string, string> ParseGetParameters(const string &text);
};

}
