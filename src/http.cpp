#include "http.hpp"

#include <algorithm>
#include <limits>
#include <optional>

namespace netcache::http {
namespace {

struct Fail {
    int status;
    std::string detail;
};

bool isTchar(unsigned char c) {
    if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) return true;
    return c != 0 && std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(c)) != std::string_view::npos;
}

bool isToken(std::string_view s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) { return isTchar(static_cast<unsigned char>(c)); });
}

bool isOws(char c) { return c == ' ' || c == '\t'; }

std::string_view trimOws(std::string_view s) {
    while (!s.empty() && isOws(s.front())) s.remove_prefix(1);
    while (!s.empty() && isOws(s.back())) s.remove_suffix(1);
    return s;
}

// Control characters (including NUL) and DEL are never valid in field values; HTAB is.
bool hasBadFieldChar(std::string_view v) {
    return std::any_of(v.begin(), v.end(), [](char ch) {
        auto c = static_cast<unsigned char>(ch);
        return (c < 0x20 && c != '\t') || c == 0x7f;
    });
}

bool isHex(char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'); }
bool isDigit(char c) { return c >= '0' && c <= '9'; }
char lowerChar(char c) { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c; }

std::string toLower(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(), lowerChar);
    return out;
}

// Every LF must be preceded by CR and every CR followed by LF.
// While a message is still arriving, a CR at the very end may be completed by the next byte.
bool hasBareLineEnding(std::string_view s, bool allowTrailingCr) {
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\n' && (i == 0 || s[i - 1] != '\r')) return true;
        if (s[i] == '\r') {
            if (i + 1 == s.size()) {
                if (!allowTrailingCr) return true;
            } else if (s[i + 1] != '\n') {
                return true;
            }
        }
    }
    return false;
}

// `lines` is a sequence of "name: value\r\n". Rejects obs-fold, bad names, bad values.
std::optional<Fail> parseHeaderLines(std::string_view lines, size_t maxHeaders, std::vector<Header>& out) {
    size_t pos = 0;
    while (pos < lines.size()) {
        size_t eol = lines.find("\r\n", pos);
        if (eol == std::string_view::npos) return Fail{400, "unterminated header line"};
        std::string_view line = lines.substr(pos, eol - pos);
        pos = eol + 2;

        if (line.empty()) return Fail{400, "empty header line"};
        if (isOws(line.front())) return Fail{400, "obsolete line folding is not accepted"};
        size_t colon = line.find(':');
        if (colon == std::string_view::npos || colon == 0) return Fail{400, "malformed header line"};
        std::string_view name = line.substr(0, colon);
        if (!isToken(name)) return Fail{400, "invalid header name"};  // also rejects "Name : v"
        std::string_view value = trimOws(line.substr(colon + 1));
        if (hasBadFieldChar(value)) return Fail{400, "control character in header value"};
        if (out.size() >= maxHeaders) return Fail{431, "too many headers"};
        out.push_back({std::string(name), std::string(value)});
    }
    return std::nullopt;
}

size_t countHeader(const std::vector<Header>& headers, std::string_view lowerName) {
    size_t n = 0;
    for (const auto& h : headers) {
        if (toLower(h.name) == lowerName) ++n;
    }
    return n;
}

const Header* findHeader(const std::vector<Header>& headers, std::string_view lowerName) {
    for (const auto& h : headers) {
        if (toLower(h.name) == lowerName) return &h;
    }
    return nullptr;
}

// Origin-form only. The path is validated but never rewritten, so what we key on is exactly
// what we forward.
std::optional<Fail> validateTarget(std::string_view t, size_t maxLen) {
    if (t.size() > maxLen) return Fail{414, "request target too long"};
    if (t.empty() || t[0] != '/') return Fail{400, "only origin-form request targets are supported"};
    if (t.size() > 1 && t[1] == '/') return Fail{400, "target begins with //"};

    size_t pathEnd = t.find('?');
    if (pathEnd == std::string_view::npos) pathEnd = t.size();

    for (size_t i = 0; i < t.size(); ++i) {
        auto c = static_cast<unsigned char>(t[i]);
        if (c <= 0x20 || c >= 0x7f) return Fail{400, "invalid character in target"};
        if (t[i] == '#' || t[i] == '\\') return Fail{400, "fragment or backslash in target"};
        if (t[i] == '%') {
            if (i + 2 >= t.size()) return Fail{400, "malformed percent-encoding"};
            if (!isHex(t[i + 1]) || !isHex(t[i + 2])) return Fail{400, "malformed percent-encoding"};
            if (t[i + 1] == '0' && t[i + 2] == '0') return Fail{400, "encoded NUL in target"};
            if (i < pathEnd && t[i + 1] == '2' && (t[i + 2] == 'e' || t[i + 2] == 'E')) {
                return Fail{400, "encoded dot in path"};
            }
        }
    }

    std::string_view path = t.substr(0, pathEnd);
    size_t start = 0;
    while (start <= path.size()) {
        size_t slash = path.find('/', start);
        std::string_view seg = path.substr(start, (slash == std::string_view::npos ? path.size() : slash) - start);
        if (seg == "." || seg == "..") return Fail{400, "dot segments are not accepted"};
        if (slash == std::string_view::npos) break;
        start = slash + 1;
    }
    return std::nullopt;
}

bool validHostValue(std::string_view v) {
    if (v.empty() || v.size() > 255) return false;
    return std::all_of(v.begin(), v.end(), [](char c) {
        return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '.' || c == '-' ||
               c == '_' || c == ':' || c == '[' || c == ']';
    });
}

enum class DecimalResult { Ok, Invalid, TooLarge };

DecimalResult parseDecimal(std::string_view s, size_t maxValue, size_t& out) {
    if (s.empty() || !std::all_of(s.begin(), s.end(), isDigit)) return DecimalResult::Invalid;
    while (s.size() > 1 && s.front() == '0') s.remove_prefix(1);
    if (s.size() > 18) return DecimalResult::TooLarge;  // 18 digits cannot overflow uint64
    unsigned long long v = 0;
    for (char c : s) v = v * 10 + static_cast<unsigned>(c - '0');
    if (v > maxValue) return DecimalResult::TooLarge;
    out = static_cast<size_t>(v);
    return DecimalResult::Ok;
}

bool isKnownMethod(std::string_view m) {
    static constexpr std::string_view kMethods[] = {"HEAD",  "POST",    "PUT",   "DELETE",
                                                    "PATCH", "OPTIONS", "TRACE", "CONNECT"};
    return std::find(std::begin(kMethods), std::end(kMethods), m) != std::end(kMethods);
}

bool looksLikeHttpVersion(std::string_view v) {
    return v.size() == 8 && v.substr(0, 5) == "HTTP/" && isDigit(v[5]) && v[6] == '.' && isDigit(v[7]);
}

}  // namespace

// ---------------------------------------------------------------- requests

RequestParseResult parseRequest(std::string_view buf, const RequestLimits& lim) {
    RequestParseResult r;
    auto fail = [&r](int status, std::string detail) {
        r.state = ParseState::Error;
        r.errorStatus = status;
        r.errorDetail = std::move(detail);
        return r;
    };

    size_t term = buf.find("\r\n\r\n");
    if (term == std::string_view::npos) {
        // Reject early: no point buffering something that is already invalid.
        if (hasBareLineEnding(buf, true)) return fail(400, "bare CR or LF");
        if (buf.size() > lim.maxHeaderBytes) return fail(431, "header block too large");
        return r;  // NeedMore
    }
    if (term + 4 > lim.maxHeaderBytes) return fail(431, "header block too large");

    std::string_view head = buf.substr(0, term + 2);  // includes the CRLF ending the last line
    if (hasBareLineEnding(head, false)) return fail(400, "bare CR or LF");

    size_t firstEol = head.find("\r\n");
    std::string_view line = head.substr(0, firstEol);

    // Request line: exactly METHOD SP TARGET SP VERSION.
    size_t s1 = line.find(' ');
    size_t s2 = s1 == std::string_view::npos ? s1 : line.find(' ', s1 + 1);
    if (s1 == std::string_view::npos || s2 == std::string_view::npos || line.find(' ', s2 + 1) != std::string_view::npos ||
        s1 == 0 || s2 == s1 + 1 || s2 + 1 == line.size()) {
        return fail(400, "malformed request line");
    }
    std::string_view method = line.substr(0, s1);
    std::string_view target = line.substr(s1 + 1, s2 - s1 - 1);
    std::string_view version = line.substr(s2 + 1);

    if (method != "GET") {
        if (!isToken(method)) return fail(400, "invalid method token");
        return fail(isKnownMethod(method) ? 405 : 501, "unsupported method");
    }
    if (version != "HTTP/1.1") {
        return fail(looksLikeHttpVersion(version) ? 505 : 400, "unsupported HTTP version");
    }
    if (auto f = validateTarget(target, lim.maxTargetBytes)) return fail(f->status, f->detail);

    std::vector<Header> headers;
    if (auto f = parseHeaderLines(head.substr(firstEol + 2), lim.maxHeaders, headers)) {
        return fail(f->status, f->detail);
    }

    // Semantic checks. A GET must have no body, so any framing header other than CL: 0 is refused.
    for (const auto& h : headers) {
        std::string n = toLower(h.name);
        if (n == "transfer-encoding") return fail(400, "Transfer-Encoding is not accepted");
        if (n == "expect") return fail(417, "Expect is not supported");
        if (n == "upgrade") return fail(501, "Upgrade is not supported");
        if (n == "content-length" && h.value != "0") return fail(400, "request bodies are not supported");
    }
    if (countHeader(headers, "content-length") > 1) return fail(400, "duplicate Content-Length");
    if (countHeader(headers, "host") != 1) return fail(400, "exactly one Host header is required");
    if (!validHostValue(findHeader(headers, "host")->value)) return fail(400, "invalid Host value");

    r.state = ParseState::Complete;
    r.consumed = term + 4;
    r.request.target = std::string(target);
    r.request.headers = std::move(headers);
    return r;
}

// --------------------------------------------------------------- responses

ResponseParseResult parseResponseHead(std::string_view buf, const ResponseLimits& lim) {
    ResponseParseResult r;
    auto fail = [&r](std::string detail) {
        r.state = ParseState::Error;
        r.errorDetail = std::move(detail);
        return r;
    };

    size_t term = buf.find("\r\n\r\n");
    if (term == std::string_view::npos) {
        if (hasBareLineEnding(buf, true)) return fail("bare CR or LF in response head");
        if (buf.size() > lim.maxHeaderBytes) return fail("response header block too large");
        return r;
    }
    if (term + 4 > lim.maxHeaderBytes) return fail("response header block too large");

    std::string_view head = buf.substr(0, term + 2);
    if (hasBareLineEnding(head, false)) return fail("bare CR or LF in response head");

    size_t firstEol = head.find("\r\n");
    std::string_view line = head.substr(0, firstEol);

    // "HTTP/1.x SP DDD [SP reason]"
    bool statusLineOk = line.size() >= 12 && line.substr(0, 7) == "HTTP/1." && (line[7] == '0' || line[7] == '1') &&
                        line[8] == ' ' && isDigit(line[9]) && isDigit(line[10]) && isDigit(line[11]) &&
                        (line.size() == 12 || line[12] == ' ');
    if (!statusLineOk) return fail("malformed status line");
    int status = (line[9] - '0') * 100 + (line[10] - '0') * 10 + (line[11] - '0');
    std::string_view reason = line.size() > 13 ? line.substr(13) : std::string_view();
    if (hasBadFieldChar(reason)) return fail("control character in reason phrase");
    if (status < 200) return fail("1xx responses are not supported");

    std::vector<Header> headers;
    if (auto f = parseHeaderLines(head.substr(firstEol + 2), lim.maxHeaders, headers)) return fail(f->detail);

    size_t teCount = countHeader(headers, "transfer-encoding");
    size_t clCount = countHeader(headers, "content-length");
    if (clCount > 1) return fail("multiple Content-Length headers");
    if (teCount > 1) return fail("multiple Transfer-Encoding headers");

    ResponseHead& h = r.head;
    h.status = status;
    h.reason = std::string(reason);
    if (status == 204 || status == 304) {
        if (teCount != 0 || clCount != 0) return fail("body framing is not allowed for this status");
        h.framing = Framing::NoBody;
        h.contentLength = 0;
    } else if (teCount == 1) {
        if (clCount != 0 || toLower(findHeader(headers, "transfer-encoding")->value) != "chunked") {
            return fail("unsupported or ambiguous Transfer-Encoding");
        }
        h.framing = Framing::Chunked;
        h.contentLength = 0;
    } else {
        if (clCount == 0) return fail("unsupported framing: no Content-Length");
        switch (parseDecimal(findHeader(headers, "content-length")->value, lim.maxBodyBytes, h.contentLength)) {
            case DecimalResult::Ok: break;
            case DecimalResult::Invalid: return fail("invalid Content-Length");
            case DecimalResult::TooLarge: return fail("response exceeds the maximum allowed size");
        }
        h.framing = Framing::ContentLength;
    }
    h.headers = std::move(headers);
    r.state = ParseState::Complete;
    r.consumed = term + 4;
    return r;
}

ChunkedDecodeResult decodeChunkedBody(std::string_view buffer, std::size_t maxBodyBytes) {
    ChunkedDecodeResult result;
    std::size_t pos = 0;
    auto fail = [&result](std::string detail) {
        result.state = ParseState::Error;
        result.errorDetail = std::move(detail);
        return result;
    };
    for (;;) {
        std::size_t lineEnd = buffer.find("\r\n", pos);
        if (lineEnd == std::string_view::npos) {
            if (buffer.size() - pos > 32) return fail("chunk size line is too long");
            return result;
        }

        std::string_view line = buffer.substr(pos, lineEnd - pos);
        if (line.empty()) return fail("empty chunk size");
        std::size_t semicolon = line.find(';');
        if (semicolon != std::string_view::npos) return fail("chunk extensions are not supported");
        if (line.size() > 16 || !std::all_of(line.begin(), line.end(), isHex)) {
            return fail("invalid chunk size");
        }
        std::size_t chunkSize = 0;
        for (char c : line) {
            unsigned digit = c <= '9' ? static_cast<unsigned>(c - '0')
                                      : static_cast<unsigned>((c >= 'a' ? c - 'a' : c - 'A') + 10);
            if (chunkSize > (std::numeric_limits<std::size_t>::max() - digit) / 16) {
                return fail("chunk size overflow");
            }
            chunkSize = chunkSize * 16 + digit;
        }
        pos = lineEnd + 2;
        if (chunkSize == 0) {
            if (buffer.size() - pos < 2) return result;
            if (buffer.substr(pos, 2) != "\r\n") return fail("chunk trailers are not supported");
            result.state = ParseState::Complete;
            result.consumed = pos + 2;
            return result;
        }
        if (result.body.size() > maxBodyBytes || chunkSize > maxBodyBytes - result.body.size()) {
            return fail("decoded response exceeds the maximum allowed size");
        }
        if (buffer.size() - pos < chunkSize) return result;
        result.body.append(buffer.substr(pos, chunkSize));
        pos += chunkSize;
        if (buffer.size() - pos < 2) return result;
        if (buffer.substr(pos, 2) != "\r\n") return fail("chunk data is not followed by CRLF");
        pos += 2;
    }
}

ChunkedDecodeResult decodeChunked(std::string_view buffer, std::size_t maxBodyBytes) {
    return decodeChunkedBody(buffer, maxBodyBytes);
}

// ------------------------------------------------------------ construction

std::string buildUpstreamRequest(const Request& request, std::string_view authority) {
    std::string out = "GET " + request.target + " HTTP/1.1\r\nHost: ";
    out.append(authority);
    out += "\r\nConnection: close\r\n";
    if (countHeader(request.headers, "accept") == 1) {
        out += "Accept: " + findHeader(request.headers, "accept")->value + "\r\n";
    }
    out += "\r\n";
    return out;
}

std::string buildClientResponse(const ResponseHead& head, std::string_view body) {
    static constexpr std::string_view kHopByHop[] = {"connection", "keep-alive",    "proxy-authenticate",
                                                     "proxy-authorization", "proxy-connection", "te",
                                                     "trailer",    "transfer-encoding", "upgrade"};
    // Headers named in Connection are hop-by-hop too (RFC 9110 7.6.1).
    std::vector<std::string> listed;
    for (const auto& h : head.headers) {
        if (toLower(h.name) != "connection") continue;
        size_t pos = 0;
        while (pos <= h.value.size()) {
            size_t comma = h.value.find(',', pos);
            size_t end = comma == std::string::npos ? h.value.size() : comma;
            std::string tok = toLower(trimOws(std::string_view(h.value).substr(pos, end - pos)));
            if (!tok.empty()) listed.push_back(std::move(tok));
            if (comma == std::string::npos) break;
            pos = comma + 1;
        }
    }

    std::string out = "HTTP/1.1 " + std::to_string(head.status) + " " +
                      (head.reason.empty() ? reasonPhrase(head.status) : head.reason) + "\r\n";
    bool hasContentLength = false;
    for (const auto& h : head.headers) {
        std::string n = toLower(h.name);
        if (n == "content-length") hasContentLength = true;
        if (n != "content-length") {  // framing is never stripped, even if a peer lists it in Connection
            if (std::find(std::begin(kHopByHop), std::end(kHopByHop), n) != std::end(kHopByHop)) continue;
            if (std::find(listed.begin(), listed.end(), n) != listed.end()) continue;
        }
        out += h.name + ": " + h.value + "\r\n";
    }
    if (head.framing == Framing::Chunked && !hasContentLength) {
        out += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    }
    out += "Connection: close\r\n\r\n";
    out.append(body);
    return out;
}

std::string setResponseHeader(std::string_view response, std::string_view name, std::string_view value) {
    size_t headEnd = response.find("\r\n\r\n");
    size_t lineEnd = response.find("\r\n");
    if (headEnd == std::string_view::npos || lineEnd == std::string_view::npos) return std::string(response);
    std::string lname = toLower(name);

    std::string out(response.substr(0, lineEnd + 2));
    out += std::string(name) + ": " + std::string(value) + "\r\n";
    size_t pos = lineEnd + 2;
    while (pos < headEnd + 2) {  // each header line ends in CRLF; headEnd + 2 is the final line's CRLF end
        size_t eol = response.find("\r\n", pos);
        std::string_view line = response.substr(pos, eol - pos);
        size_t colon = line.find(':');
        bool same = colon != std::string_view::npos && toLower(line.substr(0, colon)) == lname;
        if (!same) {
            out.append(line);
            out += "\r\n";
        }
        pos = eol + 2;
    }
    out.append(response.substr(headEnd + 2));  // the blank line (CRLF) and the body
    return out;
}

std::string buildErrorResponse(int status) {
    std::string body = std::to_string(status) + " " + reasonPhrase(status) + "\n";
    std::string out = "HTTP/1.1 " + std::to_string(status) + " " + reasonPhrase(status) + "\r\n";
    if (status == 405) out += "Allow: GET\r\n";
    out += "Content-Type: text/plain; charset=utf-8\r\n";
    out += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    out += "Connection: close\r\n\r\n";
    out += body;
    return out;
}

const char* reasonPhrase(int status) {
    switch (status) {
        case 200: return "OK";
        case 204: return "No Content";
        case 304: return "Not Modified";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 408: return "Request Timeout";
        case 414: return "URI Too Long";
        case 417: return "Expectation Failed";
        case 431: return "Request Header Fields Too Large";
        case 501: return "Not Implemented";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        case 504: return "Gateway Timeout";
        case 505: return "HTTP Version Not Supported";
        default: return "Status";
    }
}

}  // namespace netcache::http
