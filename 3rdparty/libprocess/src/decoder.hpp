#ifndef __DECODER_HPP__
#define __DECODER_HPP__

#include <http_parser.h>

#include <glog/logging.h>

#include <deque>
#include <string>
#include <vector>

#include <process/http.hpp>
#include <process/socket.hpp>

#include <stout/foreach.hpp>
#include <stout/gzip.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>


// TODO(bmahler): Switch to joyent/http-parser now that it is no
// longer being hosted under ry/http-parser.

namespace process {

// TODO(benh): Make DataDecoder abstract and make RequestDecoder a
// concrete subclass.
class DataDecoder
{
public:
  explicit DataDecoder(const network::Socket& _s)
    : s(_s), failure(false), request(NULL)
  {
    settings.on_message_begin = &DataDecoder::onMessageBegin;

#if !(HTTP_PARSER_VERSION_MAJOR >= 2)
    settings.on_path = &DataDecoder::onPath;
    settings.on_fragment = &DataDecoder::onFragment;
    settings.on_query_string = &DataDecoder::onQueryString;
#endif

    settings.on_url = &DataDecoder::onUrl;
    settings.on_header_field = &DataDecoder::onHeaderField;
    settings.on_header_value = &DataDecoder::onHeaderValue;
    settings.on_headers_complete = &DataDecoder::onHeadersComplete;
    settings.on_body = &DataDecoder::onBody;
    settings.on_message_complete = &DataDecoder::onMessageComplete;

    http_parser_init(&parser, HTTP_REQUEST);

    parser.data = this;
  }

  std::deque<http::Request*> decode(const char* data, size_t length)
  {
    size_t parsed = http_parser_execute(&parser, &settings, data, length);

    if (parsed != length) {
      // TODO(bmahler): joyent/http-parser exposes error reasons.
      failure = true;
    }

    if (!requests.empty()) {
      std::deque<http::Request*> result = requests;
      requests.clear();
      return result;
    }

    return std::deque<http::Request*>();
  }

  bool failed() const
  {
    return failure;
  }

  network::Socket socket() const
  {
    return s;
  }

private:
  static int onMessageBegin(http_parser* p)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;

    CHECK(!decoder->failure);

    decoder->header = HEADER_FIELD;
    decoder->field.clear();
    decoder->value.clear();
    decoder->query.clear();

    CHECK(decoder->request == NULL);

    decoder->request = new http::Request();
    decoder->request->headers.clear();
    decoder->request->method.clear();
    decoder->request->path.clear();
    decoder->request->url.clear();
    decoder->request->fragment.clear();
    decoder->request->query.clear();
    decoder->request->body.clear();

    return 0;
  }

#if !(HTTP_PARSER_VERSION_MAJOR >= 2)
  static int onPath(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);
    decoder->request->path.append(data, length);
    return 0;
  }

  static int onQueryString(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);
    decoder->query.append(data, length);
    return 0;
  }

  static int onFragment(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);
    decoder->request->fragment.append(data, length);
    return 0;
  }
#endif // !(HTTP_PARSER_VERSION_MAJOR >= 2)

  static int onUrl(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);
    decoder->request->url.append(data, length);
    int result = 0;

#if (HTTP_PARSER_VERSION_MAJOR >= 2)
    // Reworked parsing for version >= 2.0.
    http_parser_url url;
    result = http_parser_parse_url(data, length, 0, &url);

    if (result == 0) {
      if (url.field_set & (1 << UF_PATH)) {
        decoder->request->path.append(
            data + url.field_data[UF_PATH].off,
            url.field_data[UF_PATH].len);
      }

      if (url.field_set & (1 << UF_FRAGMENT)) {
        decoder->request->fragment.append(
            data + url.field_data[UF_FRAGMENT].off,
            url.field_data[UF_FRAGMENT].len);
      }

      if (url.field_set & (1 << UF_QUERY)) {
        decoder->query.append(
            data + url.field_data[UF_QUERY].off,
            url.field_data[UF_QUERY].len);
      }
    }
#endif

    return result;
  }

  static int onHeaderField(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);

    if (decoder->header != HEADER_FIELD) {
      decoder->request->headers[decoder->field] = decoder->value;
      decoder->field.clear();
      decoder->value.clear();
    }

    decoder->field.append(data, length);
    decoder->header = HEADER_FIELD;

    return 0;
  }

  static int onHeaderValue(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);
    decoder->value.append(data, length);
    decoder->header = HEADER_VALUE;
    return 0;
  }

  static int onHeadersComplete(http_parser* p)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;

    CHECK_NOTNULL(decoder->request);

    // Add final header.
    decoder->request->headers[decoder->field] = decoder->value;
    decoder->field.clear();
    decoder->value.clear();

    decoder->request->method =
      http_method_str((http_method) decoder->parser.method);

    decoder->request->keepAlive = http_should_keep_alive(&decoder->parser);

    return 0;
  }

  static int onBody(http_parser* p, const char* data, size_t length)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;
    CHECK_NOTNULL(decoder->request);
    decoder->request->body.append(data, length);
    return 0;
  }

  static int onMessageComplete(http_parser* p)
  {
    DataDecoder* decoder = (DataDecoder*) p->data;

    // Parse the query key/values.
    Try<hashmap<std::string, std::string>> decoded =
      http::query::decode(decoder->query);

    if (decoded.isError()) {
      return 1;
    }

    CHECK_NOTNULL(decoder->request);

    decoder->request->query =  decoded.get();

    Option<std::string> encoding =
      decoder->request->headers.get("Content-Encoding");

    if (encoding.isSome() && encoding.get() == "gzip") {
      Try<std::string> decompressed = gzip::decompress(decoder->request->body);
      if (decompressed.isError()) {
        return 1;
      }
      decoder->request->body = decompressed.get();
      decoder->request->headers["Content-Length"] =
        decoder->request->body.length();
    }

    decoder->requests.push_back(decoder->request);
    decoder->request = NULL;
    return 0;
  }

  const network::Socket s; // The socket this decoder is associated with.

  bool failure;

  http_parser parser;
  http_parser_settings settings;

  enum {
    HEADER_FIELD,
    HEADER_VALUE
  } header;

  std::string field;
  std::string value;
  std::string query;

  http::Request* request;

  std::deque<http::Request*> requests;
};


class ResponseDecoder
{
public:
  ResponseDecoder()
    : failure(false), header(HEADER_FIELD), response(NULL)
  {
    settings.on_message_begin = &ResponseDecoder::onMessageBegin;

#if !(HTTP_PARSER_VERSION_MAJOR >=2)
    settings.on_path = &ResponseDecoder::onPath;
    settings.on_fragment = &ResponseDecoder::onFragment;
    settings.on_query_string = &ResponseDecoder::onQueryString;
#endif

    settings.on_url = &ResponseDecoder::onUrl;
    settings.on_header_field = &ResponseDecoder::onHeaderField;
    settings.on_header_value = &ResponseDecoder::onHeaderValue;
    settings.on_headers_complete = &ResponseDecoder::onHeadersComplete;
    settings.on_body = &ResponseDecoder::onBody;
    settings.on_message_complete = &ResponseDecoder::onMessageComplete;

    http_parser_init(&parser, HTTP_RESPONSE);

    parser.data = this;
  }

  std::deque<http::Response*> decode(const char* data, size_t length)
  {
    size_t parsed = http_parser_execute(&parser, &settings, data, length);

    if (parsed != length) {
      // TODO(bmahler): joyent/http-parser exposes error reasons.
      failure = true;
    }

    if (!responses.empty()) {
      std::deque<http::Response*> result = responses;
      responses.clear();
      return result;
    }

    return std::deque<http::Response*>();
  }

  bool failed() const
  {
    return failure;
  }

private:
  static int onMessageBegin(http_parser* p)
  {
    ResponseDecoder* decoder = (ResponseDecoder*) p->data;

    CHECK(!decoder->failure);

    decoder->header = HEADER_FIELD;
    decoder->field.clear();
    decoder->value.clear();

    CHECK(decoder->response == NULL);

    decoder->response = new http::Response();
    decoder->response->status.clear();
    decoder->response->headers.clear();
    decoder->response->type = http::Response::BODY;
    decoder->response->body.clear();
    decoder->response->path.clear();

    return 0;
  }

#if !(HTTP_PARSER_VERSION_MAJOR >= 2)
  static int onPath(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }

  static int onQueryString(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }

  static int onFragment(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }
#endif // !(HTTP_PARSER_VERSION_MAJOR >= 2)

  static int onUrl(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }

  static int onHeaderField(http_parser* p, const char* data, size_t length)
  {
    ResponseDecoder* decoder = (ResponseDecoder*) p->data;
    CHECK_NOTNULL(decoder->response);

    if (decoder->header != HEADER_FIELD) {
      decoder->response->headers[decoder->field] = decoder->value;
      decoder->field.clear();
      decoder->value.clear();
    }

    decoder->field.append(data, length);
    decoder->header = HEADER_FIELD;

    return 0;
  }

  static int onHeaderValue(http_parser* p, const char* data, size_t length)
  {
    ResponseDecoder* decoder = (ResponseDecoder*) p->data;
    CHECK_NOTNULL(decoder->response);
    decoder->value.append(data, length);
    decoder->header = HEADER_VALUE;
    return 0;
  }

  static int onHeadersComplete(http_parser* p)
  {
    ResponseDecoder* decoder = (ResponseDecoder*) p->data;

    CHECK_NOTNULL(decoder->response);

    // Add final header.
    decoder->response->headers[decoder->field] = decoder->value;
    decoder->field.clear();
    decoder->value.clear();

    return 0;
  }

  static int onBody(http_parser* p, const char* data, size_t length)
  {
    ResponseDecoder* decoder = (ResponseDecoder*) p->data;
    CHECK_NOTNULL(decoder->response);
    decoder->response->body.append(data, length);
    return 0;
  }

  static int onMessageComplete(http_parser* p)
  {
    ResponseDecoder* decoder = (ResponseDecoder*) p->data;

    CHECK_NOTNULL(decoder->response);

    // Get the response status string.
    if (http::statuses.contains(decoder->parser.status_code)) {
      decoder->response->status = http::statuses[decoder->parser.status_code];
    } else {
      decoder->failure = true;
      return 1;
    }

    // We can only provide the gzip encoding.
    Option<std::string> encoding =
      decoder->response->headers.get("Content-Encoding");
    if (encoding.isSome() && encoding.get() == "gzip") {
      Try<std::string> decompressed = gzip::decompress(decoder->response->body);
      if (decompressed.isError()) {
        decoder->failure = true;
        return 1;
      }
      decoder->response->body = decompressed.get();
      decoder->response->headers["Content-Length"] =
        decoder->response->body.length();
    }

    decoder->responses.push_back(decoder->response);
    decoder->response = NULL;
    return 0;
  }

  bool failure;

  http_parser parser;
  http_parser_settings settings;

  enum {
    HEADER_FIELD,
    HEADER_VALUE
  } header;

  std::string field;
  std::string value;

  http::Response* response;

  std::deque<http::Response*> responses;
};


// Provides a response decoder that returns 'PIPE' responses once
// the response headers are received, but before the body data
// is received. Callers are expected to read the body from the
// Pipe::Reader in the response.
//
// TODO(bmahler): Consolidate streaming and non-streaming response
// decoders.
class StreamingResponseDecoder
{
public:
  StreamingResponseDecoder()
    : failure(false), header(HEADER_FIELD), response(NULL)
  {
    settings.on_message_begin =
      &StreamingResponseDecoder::onMessageBegin;

#if !(HTTP_PARSER_VERSION_MAJOR >=2)
    settings.on_path =
      &StreamingResponseDecoder::onPath;
    settings.on_fragment =
      &StreamingResponseDecoder::onFragment;
    settings.on_query_string =
      &StreamingResponseDecoder::onQueryString;
#endif

    settings.on_url =
      &StreamingResponseDecoder::onUrl;
    settings.on_header_field =
      &StreamingResponseDecoder::onHeaderField;
    settings.on_header_value =
      &StreamingResponseDecoder::onHeaderValue;
    settings.on_headers_complete =
      &StreamingResponseDecoder::onHeadersComplete;
    settings.on_body =
      &StreamingResponseDecoder::onBody;
    settings.on_message_complete =
      &StreamingResponseDecoder::onMessageComplete;

    http_parser_init(&parser, HTTP_RESPONSE);

    parser.data = this;
  }

  std::deque<http::Response*> decode(const char* data, size_t length)
  {
    size_t parsed = http_parser_execute(&parser, &settings, data, length);

    if (parsed != length) {
      // TODO(bmahler): joyent/http-parser exposes error reasons.
      failure = true;

      // If we're still writing the body, fail the writer!
      if (writer.isSome()) {
        http::Pipe::Writer writer_ = writer.get(); // Remove const.
        writer_.fail("failed to decode body");
        writer = None();
      }
    }

    if (!responses.empty()) {
      std::deque<http::Response*> result = responses;
      responses.clear();
      return result;
    }

    return std::deque<http::Response*>();
  }

  bool failed() const
  {
    return failure;
  }

private:
  static int onMessageBegin(http_parser* p)
  {
    StreamingResponseDecoder* decoder = (StreamingResponseDecoder*) p->data;

    CHECK(!decoder->failure);

    decoder->header = HEADER_FIELD;
    decoder->field.clear();
    decoder->value.clear();

    CHECK(decoder->response == NULL);
    CHECK(decoder->writer.isNone());

    decoder->response = new http::Response();
    decoder->response->type = http::Response::PIPE;
    decoder->writer = None();

    return 0;
  }

#if !(HTTP_PARSER_VERSION_MAJOR >= 2)
  static int onPath(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }

  static int onQueryString(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }

  static int onFragment(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }
#endif // !(HTTP_PARSER_VERSION_MAJOR >= 2)

  static int onUrl(http_parser* p, const char* data, size_t length)
  {
    return 0;
  }

  static int onHeaderField(http_parser* p, const char* data, size_t length)
  {
    StreamingResponseDecoder* decoder = (StreamingResponseDecoder*) p->data;

    CHECK_NOTNULL(decoder->response);

    if (decoder->header != HEADER_FIELD) {
      decoder->response->headers[decoder->field] = decoder->value;
      decoder->field.clear();
      decoder->value.clear();
    }

    decoder->field.append(data, length);
    decoder->header = HEADER_FIELD;

    return 0;
  }

  static int onHeaderValue(http_parser* p, const char* data, size_t length)
  {
    StreamingResponseDecoder* decoder = (StreamingResponseDecoder*) p->data;

    CHECK_NOTNULL(decoder->response);

    decoder->value.append(data, length);
    decoder->header = HEADER_VALUE;
    return 0;
  }

  static int onHeadersComplete(http_parser* p)
  {
    StreamingResponseDecoder* decoder = (StreamingResponseDecoder*) p->data;

    CHECK_NOTNULL(decoder->response);

    // Add final header.
    decoder->response->headers[decoder->field] = decoder->value;
    decoder->field.clear();
    decoder->value.clear();

    // Get the response status string.
    if (http::statuses.contains(decoder->parser.status_code)) {
      decoder->response->status = http::statuses[decoder->parser.status_code];
    } else {
      decoder->failure = true;
      return 1;
    }

    // We cannot provide streaming gzip decompression!
    Option<std::string> encoding =
      decoder->response->headers.get("Content-Encoding");
    if (encoding.isSome() && encoding.get() == "gzip") {
      decoder->failure = true;
      return 1;
    }

    CHECK(decoder->writer.isNone());

    http::Pipe pipe;
    decoder->writer = pipe.writer();
    decoder->response->reader = pipe.reader();

    // Send the response to the caller, but keep a Pipe::Writer for
    // streaming the body content into the response.
    decoder->responses.push_back(decoder->response);
    decoder->response = NULL;

    return 0;
  }

  static int onBody(http_parser* p, const char* data, size_t length)
  {
    StreamingResponseDecoder* decoder = (StreamingResponseDecoder*) p->data;

    CHECK_SOME(decoder->writer);

    http::Pipe::Writer writer = decoder->writer.get(); // Remove const.
    writer.write(std::string(data, length));

    return 0;
  }

  static int onMessageComplete(http_parser* p)
  {
    StreamingResponseDecoder* decoder = (StreamingResponseDecoder*) p->data;

    CHECK_SOME(decoder->writer);

    http::Pipe::Writer writer = decoder->writer.get(); // Remove const.
    writer.close();

    decoder->writer = None();

    return 0;
  }

  bool failure;

  http_parser parser;
  http_parser_settings settings;

  enum {
    HEADER_FIELD,
    HEADER_VALUE
  } header;

  std::string field;
  std::string value;

  http::Response* response;
  Option<http::Pipe::Writer> writer;

  std::deque<http::Response*> responses;
};

}  // namespace process {

#endif // __DECODER_HPP__
