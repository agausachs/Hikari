﻿using log4net;
using log4net.Config;
using System;
using System.Reflection;
/**
* 命名空间: Hikari 
* 类 名： Logger
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

[assembly: XmlConfigurator(ConfigFile = "log4net.config", Watch = true)]
namespace Hikari
{
    /// <summary>
    /// 功能描述    ：Logger  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/10/26 20:44:15 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/10/26 20:44:15 
    /// </summary>

    public sealed class Logger
    {
        #region [ 单例模式 ]

        private static Logger logger;
        private static readonly log4net.ILog _Logger4net = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>  
        /// 无参私有构造函数  
        /// </summary>  
        private Logger()
        {
        }

        /// <summary>  
        /// 得到单例  
        /// </summary>  
        public static Logger Singleton
        {
            get
            {
                if (logger == null)
                {
                    logger = new Logger();
                }
                return logger;
            }
        }

        /// <summary>
        /// 设置配置
        /// </summary>
        /// <param name="path"></param>
        public void LogConfiguration(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return;
            }
            try
            {
                XmlConfigurator.ConfigureAndWatch(null, new System.IO.FileInfo(path));
            }
            catch (Exception ex)
            {
                //Console.WriteLine("Error:配置日志错误," + ex.Message);
                Console.WriteLine("Error:Configuration log error," + ex.Message);
            }
        }
        #endregion

        #region [ 参数 ]

        #region [ parameter ]

        public bool IsDebugEnabled
        {
            get { return _Logger4net.IsDebugEnabled; }
        }
        public bool IsInfoEnabled
        {
            get { return _Logger4net.IsInfoEnabled; }
        }
        public bool IsWarnEnabled
        {
            get { return _Logger4net.IsWarnEnabled; }
        }
        public bool IsErrorEnabled
        {
            get { return _Logger4net.IsErrorEnabled; }
        }
        public bool IsFatalEnabled
        {
            get { return _Logger4net.IsFatalEnabled; }
        }

        #endregion

        #region [ 接口方法 ]

        public void Debug(string message)
        {
            if (this.IsDebugEnabled)
            {
                this.Log(LogLevel.Debug, message);
            }
        }

        public void Debug(string message, Exception exception)
        {
            if (this.IsDebugEnabled)
            {
                this.Log(LogLevel.Debug, message, exception);
            }
        }

        public void DebugFormat(string format, params object[] args)
        {
            if (this.IsDebugEnabled)
            {
                this.Log(LogLevel.Debug, format, args);
            }
        }

        public void DebugFormat(string format, Exception exception, params object[] args)
        {
            if (this.IsDebugEnabled)
            {
                this.Log(LogLevel.Debug, string.Format(format, args), exception);
            }
        }

        #endregion

        #region [ Info ]

        public void Info(string message)
        {
            if (this.IsInfoEnabled)
            {
                this.Log(LogLevel.Info, message);
            }
        }

        public void Info(string message, Exception exception)
        {
            if (this.IsInfoEnabled)
            {
                this.Log(LogLevel.Info, message, exception);
            }
        }

        public void InfoFormat(string format, params object[] args)
        {
            if (this.IsInfoEnabled)
            {
                this.Log(LogLevel.Info, format, args);
            }
        }

        public void InfoFormat(string format, Exception exception, params object[] args)
        {
            if (this.IsInfoEnabled)
            {
                this.Log(LogLevel.Info, string.Format(format, args), exception);
            }
        }

        #endregion

        #region  [ Warn ]

        public void Warn(string message)
        {
            if (this.IsWarnEnabled)
            {
                this.Log(LogLevel.Warn, message);
            }
        }

        public void Warn(string message, Exception exception)
        {
            if (this.IsWarnEnabled)
            {
                this.Log(LogLevel.Warn, message, exception);
            }
        }

        public void WarnFormat(string format, params object[] args)
        {
            if (this.IsWarnEnabled)
            {
                this.Log(LogLevel.Warn, format, args);
            }
        }

        public void WarnFormat(string format, Exception exception, params object[] args)
        {
            if (this.IsWarnEnabled)
            {
                this.Log(LogLevel.Warn, string.Format(format, args), exception);
            }
        }

        #endregion

        #region  [ Error ]

        public void Error(string message)
        {
            if (this.IsErrorEnabled)
            {
                this.Log(LogLevel.Error, message);
            }
        }

        public void Error(string message, Exception exception)
        {
            if (this.IsErrorEnabled)
            {
                this.Log(LogLevel.Error, message, exception);
            }
        }

        public void ErrorFormat(string format, params object[] args)
        {
            if (this.IsErrorEnabled)
            {
                this.Log(LogLevel.Error, format, args);
            }
        }

        public void ErrorFormat(string format, Exception exception, params object[] args)
        {
            if (this.IsErrorEnabled)
            {
                this.Log(LogLevel.Error, string.Format(format, args), exception);
            }
        }
        #endregion

        #region  [ Fatal ]

        public void Fatal(string message)
        {
            if (this.IsFatalEnabled)
            {
                this.Log(LogLevel.Fatal, message);
            }
        }

        public void Fatal(string message, Exception exception)
        {
            if (this.IsFatalEnabled)
            {
                this.Log(LogLevel.Fatal, message, exception);
            }
        }

        public void FatalFormat(string format, params object[] args)
        {
            if (this.IsFatalEnabled)
            {
                this.Log(LogLevel.Fatal, format, args);
            }
        }

        public void FatalFormat(string format, Exception exception, params object[] args)
        {
            if (this.IsFatalEnabled)
            {
                this.Log(LogLevel.Fatal, string.Format(format, args), exception);
            }
        }
        #endregion

        #endregion

        #region [ 内部方法 ]  
        /// <summary>  
        /// 输出普通日志  
        /// 
        /// Output general log
        /// </summary>  
        /// <param name="level"></param>  
        /// <param name="format"></param>  
        /// <param name="args"></param>  
        private void Log(LogLevel level, string format, params object[] args)
        {
            switch (level)
            {
                case LogLevel.Debug:
                    _Logger4net.DebugFormat(format, args);
                    break;
                case LogLevel.Info:
                    _Logger4net.InfoFormat(format, args);
                    break;
                case LogLevel.Warn:
                    _Logger4net.WarnFormat(format, args);
                    break;
                case LogLevel.Error:
                    _Logger4net.ErrorFormat(format, args);
                    break;
                case LogLevel.Fatal:
                    _Logger4net.FatalFormat(format, args);
                    break;
            }
        }

        /// <summary>  
        /// 格式化输出异常信息  
        /// 
        /// Format and output exception information
        /// </summary>  
        /// <param name="level"></param>  
        /// <param name="message"></param>  
        /// <param name="exception"></param>  
        private void Log(LogLevel level, string message, Exception exception)
        {
            switch (level)
            {
                case LogLevel.Debug:
                    _Logger4net.Debug(message, exception);
                    break;
                case LogLevel.Info:
                    _Logger4net.Info(message, exception);
                    break;
                case LogLevel.Warn:
                    _Logger4net.Warn(message, exception);
                    break;
                case LogLevel.Error:
                    _Logger4net.Error(message, exception);
                    break;
                case LogLevel.Fatal:
                    _Logger4net.Fatal(message, exception);
                    break;
            }
        }

        #endregion
    }


}
