#include <cstdlib>
#include <iostream>

#include "ray/util/logging.h"

#ifdef RAY_USE_GLOG
#include "glog/logging.h"
#elif RAY_USE_LOG4CPLUS
#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/layout.h>
#include <log4cplus/logger.h>
#include <log4cplus/loggingmacros.h>
#endif

namespace ray {

// This is the default implementation of ray log,
// which is independent of any libs.
class CerrLog {
 public:
  CerrLog(int severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == RAY_FATAL) {
      PrintBackTrace();
      std::abort();
    }
  }

  std::ostream &Stream() {
    has_logged_ = true;
    return std::cerr;
  }

  template <class T>
  CerrLog &operator<<(const T &t) {
    if (severity_ != RAY_DEBUG) {
      has_logged_ = true;
      std::cerr << t;
    }
    return *this;
  }

 protected:
  const int severity_;
  bool has_logged_;

  void PrintBackTrace() {
#if defined(_EXECINFO_H) || !defined(_WIN32)
    void *buffer[255];
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

int RayLog::severity_threshold_ = RAY_INFO;
std::string RayLog::app_name_ = "";

#ifdef RAY_USE_GLOG
using namespace google;

// Glog's severity map.
static int GetMappedSeverity(int severity) {
  switch (severity) {
  case RAY_DEBUG:
    return GLOG_INFO;
  case RAY_INFO:
    return GLOG_INFO;
  case RAY_WARNING:
    return GLOG_WARNING;
  case RAY_ERROR:
    return GLOG_ERROR;
  case RAY_FATAL:
    return GLOG_FATAL;
  default:
    RAY_LOG(FATAL) << "Unsupported logging level: " << severity;
    // This return won't be hit but compiler needs it.
    return GLOG_FATAL;
  }
}

#elif RAY_USE_LOG4CPLUS
using namespace log4cplus;
using namespace log4cplus::helpers;
// Log4cplus's severity map.
static int GetMappedSeverity(int severity) {
  static int severity_map[] = {
      DEBUG_LOG_LEVEL,  // RAY_DEBUG
      INFO_LOG_LEVEL,   // RAY_INFO
      WARN_LOG_LEVEL,   // RAY_WARNING
      ERROR_LOG_LEVEL,  // RAY_ERROR
      FATAL_LOG_LEVEL   // RAY_FATAL
  };
  // Ray log level starts from -1 (RAY_DEBUG);
  return severity_map[severity + 1];
}
// This is a helper class for log4cplus.
// Log4cplus needs initialized, so the ctor will do the default initialization.
class Log4cplusHelper {
 public:
  Log4cplusHelper() {
    // This is the function to setup a default log4cplus log.
    // `static Log4cplusHelper log4cplus_initializer;` is used to trigger this function.
    AddConsoleAppender(static_logger, "default console appender");
    static_logger.setLogLevel(ALL_LOG_LEVEL);
  }
   static std::string GetDafaultPatternString() {
    // Default log format.
    return "%d{%Y-%m-%d %H-%M-%S} [%l]: %m%n";
  }
   static void AddConsoleAppender(log4cplus::Logger &logger,
                                 const std::string &appender_name) {
    SharedObjectPtr<Appender> appender(new ConsoleAppender());
    appender->setName(appender_name);
    std::unique_ptr<Layout> layout(new PatternLayout(GetDafaultPatternString()));
    appender->setLayout(std::move(layout));
    logger.addAppender(appender);
  }
   static void AddFileAppender(log4cplus::Logger &logger, const std::string &app_name,
                              const std::string &log_dir) {
    SharedObjectPtr<Appender> appender(
        new DailyRollingFileAppender(log_dir + app_name, DAILY, true, 50));
    std::unique_ptr<Layout> layout(new PatternLayout(GetDafaultPatternString()));
    appender->setName(app_name);
    appender->setLayout(std::move(layout));
    logger.addAppender(appender);
  }
   static void InitLog4cplus(const std::string &app_name, int severity_threshold,
                            const std::string &log_dir) {
    static_logger = Logger::getInstance(app_name);
    AddConsoleAppender(static_logger,
                       (std::string(app_name) + "console appender").c_str());
    if (!log_dir.empty()) {
      AddFileAppender(static_logger, app_name, log_dir);
    }
    static_logger.setLogLevel(GetMappedSeverity(severity_threshold));
  }
  static log4cplus::Logger GetLogger() { return static_logger; }
 private:
  static log4cplus::Logger static_logger;
  static int severity_map[];
};
log4cplus::Logger Log4cplusHelper::static_logger = Logger::getInstance("default");
static Log4cplusHelper log4cplus_initializer;
#endif

void RayLog::StartRayLog(const std::string &app_name, int severity_threshold,
                         const std::string &log_dir) {
#ifdef RAY_USE_GLOG
  severity_threshold_ = severity_threshold;
  app_name_ = app_name;
  int mapped_severity_threshold = GetMappedSeverity(severity_threshold_);
  google::InitGoogleLogging(app_name_.c_str());
  google::SetStderrLogging(mapped_severity_threshold);
  // Enble log file if log_dir is not empty.
  if (!log_dir.empty()) {
    auto dir_ends_with_slash = log_dir;
    if (log_dir[log_dir.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
    auto app_name_without_path = app_name;
    if (app_name.empty()) {
      app_name_without_path = "DefaultApp";
    } else {
      // Find the app name without the path.
      size_t pos = app_name.rfind('/');
      if (pos != app_name.npos && pos + 1 < app_name.length()) {
        app_name_without_path = app_name.substr(pos + 1);
      }
    }
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    google::SetLogDestination(mapped_severity_threshold, log_dir.c_str());
  }
#elif RAY_USE_LOG4CPLUS
  Log4cplusHelper::InitLog4cplus(app_name, severity_threshold_, log_dir);
#endif
}

void RayLog::ShutDownRayLog() {
#ifdef RAY_USE_GLOG
  google::ShutdownGoogleLogging();
#endif
}

void RayLog::InstallFailureSignalHandler() {
#ifdef RAY_USE_GLOG
  google::InstallFailureSignalHandler();
#endif
}

bool RayLog::IsLevelEnabled(int log_level) { return log_level >= severity_threshold_; }

RayLog::RayLog(const char *file_name, int line_number, int severity)
    // glog does not have DEBUG level, we can handle it here.
    : is_enabled_(severity >= severity_threshold_), severity_(severity),
      line_number_(line_number), file_name_(file_name) {
#ifdef RAY_USE_GLOG
  if (is_enabled_) {
    logging_provider_.reset(
        new google::LogMessage(file_name, line_number, GetMappedSeverity(severity)));
  }
#elif RAY_USE_LOG4CPLUS
  logging_provider_.reset(new std::ostringstream());
#else
  logging_provider_.reset(new CerrLog(severity));
  *logging_provider_ << file_name << ":" << line_number << ": ";
#endif
}

std::ostream &RayLog::Stream() {
#ifdef RAY_USE_GLOG
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider_->stream();
#elif RAY_USE_LOG4CPLUS
  return *logging_provider_;
#else
  return logging_provider_->Stream();
#endif
}

bool RayLog::IsEnabled() const { return is_enabled_; }

RayLog::~RayLog() {
#ifdef RAY_USE_LOG4CPLUS
  log4cplus::Logger logger = Log4cplusHelper::GetLogger();
  int mapped_severity = GetMappedSeverity(severity_);
  if (severity_ >= severity_threshold_) {
    log4cplus::detail::macro_forced_log(
        logger, mapped_severity, logging_provider_->str(),
        file_name_, line_number_, nullptr);
  }
  // Log4cplus won't exit at fatal level.
  if (severity_ == RAY_FATAL) {
    std::abort();
  }
#else
  // Avoid the compiling error of unused variable.
  RAY_IGNORE_EXPR(severity_);
  RAY_IGNORE_EXPR(file_name_);
  RAY_IGNORE_EXPR(line_number_);
#endif
  logging_provider_.reset(); }
}  // namespace ray
