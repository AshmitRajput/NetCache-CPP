#pragma once
// Strict HTTP/1.1 subset: parsing and message construction.
// Pure functions over byte buffers: no sockets, no I/O, no global state.
// Design rule: reject ambiguity, never repair framing.

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace netcache::http {

struct Header {
    std::string name;   // original case preserved
    std::string value;  // surrounding whitespace trimmed
};

enum class ParseState { NeedMore, Complete, Error };

// ---------------------------------------------------------------- requests

struct RequestLimits {
    size_t maxHeaderBytes = 8 * 1024;  // request line + headers + terminating CRLFCRLF
    size_t maxHeaders = 64;
    size_t maxTargetBytes = 2048;
};

struct Request {
    std::string target;  // origin-form, exactly as received (never decoded or normalized)
    std::vector<Header> headers;
};

struct RequestParseResult {
    ParseState state = ParseState::NeedMore;
    int errorStatus = 0;        // HTTP status to answer with when state == Error
    std::string errorDetail;    // for logs only; never sent to the client
    size_t consumed = 0;        // bytes of the buffer that make up the request head
    Request request;
};

// Incremental: call again with a longer buffer on NeedMore.
// Only GET / HTTP/1.1 without a body is accepted.
RequestParseResult parseRequest(std::string_view buffer, const RequestLimits& limits = {});

// --------------------------------------------------------------- responses

// Phase 3 adds Chunked here.
enum class Framing { ContentLength, NoBody };

struct ResponseLimits {
    size_t maxHeaderBytes = 16 * 1024;
    size_t maxHeaders = 100;
    size_t maxBodyBytes = 1024 * 1024;
};

struct ResponseHead {
    int status = 0;
    std::string reason;
    std::vector<Header> headers;
    Framing framing = Framing::ContentLength;
    size_t contentLength = 0;  // 0 when framing == NoBody
};

struct ResponseParseResult {
    ParseState state = ParseState::NeedMore;
    std::string errorDetail;  // for logs only
    size_t consumed = 0;      // bytes of the buffer that make up the response head
    ResponseHead head;
};

ResponseParseResult parseResponseHead(std::string_view buffer, const ResponseLimits& limits = {});

// ------------------------------------------------------------ construction

// Request sent upstream is rebuilt from scratch: fixed Host, Connection: close,
// and only a single well-formed Accept header is carried over from the client.
std::string buildUpstreamRequest(const Request& request, std::string_view upstreamAuthority);

// Response for the client: hop-by-hop headers removed, Connection: close added.
// Framing headers (Content-Length) are preserved exactly, never rewritten.
std::string buildClientResponse(const ResponseHead& head, std::string_view body);

// Returns `response` (a complete response WE generated: head + body) with every existing header
// called `name` removed and `name: value` inserted right after the status line. Used to stamp
// X-Cache. Only the head is touched; the body bytes are copied verbatim. If the input has no
// CRLF CRLF head terminator it is returned unchanged.
std::string setResponseHeader(std::string_view response, std::string_view name, std::string_view value);

// Minimal generated error response with a fixed body (no upstream detail leaks).
std::string buildErrorResponse(int status);

const char* reasonPhrase(int status);

}  // namespace netcache::http
