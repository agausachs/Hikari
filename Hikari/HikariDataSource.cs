using Hikari.BulkCopy;
using System;
using System.Data;
using System.Threading;
/**
* 命名空间: Hikari 
* 类 名： HikariDataSource
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace Hikari
{

    /// <summary>
    /// 功能描述    ：HikariDataSource  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/10/24 15:21:09 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/10/24 15:21:09 
    /// </summary>
    public class HikariDataSource:HikariConfig,IDisposable
    {

        /// <summary>
        /// 连接池，初始化时使用，用于锁定
        /// 
        /// Connection pool, used during initialization, for locking
        /// </summary>
        private HikariPool pool = null;

        /// <summary>
        /// 是否关闭
        /// 
        /// Is it closed?
        /// </summary>
        private volatile  bool isShutdown = false;

        /// <summary>
        /// 连接池,常用对象
        /// 
        /// Connection pool, commonly used objects
        /// </summary>
        private HikariPool fastPathPool;

        /// <summary>
        /// 是否初始化
        /// 
        /// Is it initialized?
        /// </summary>
        private volatile bool isInit = true;//需要加载

        /// <summary>
        /// 状态
        /// 
        /// state
        /// </summary>
        public bool IsClosed
        {
            get { return isShutdown; }

        }


        /// <summary>
        /// 连接提供DataSource
        /// 
        /// Connect to provide DataSource
        /// </summary>
        /// <param name="configuration"></param>
        public HikariDataSource(HikariConfig configuration)
        {
            configuration.Validate();
            configuration.CopyStateTo(this);
            Logger.Singleton.InfoFormat("{0} - Starting...", configuration.PoolName);
            pool = fastPathPool = new HikariPool(this);
            Logger.Singleton.InfoFormat("{0} - Start completed.", configuration.PoolName);
        }

        /// <summary>
        /// 连接提供DataSource
        /// 
        /// Connect to provide DataSource
        /// </summary>
        public HikariDataSource():base()
        {
            fastPathPool = null;
        }

        /// <summary>
        /// 销毁资源
        /// </summary>
        public void Dispose()
        {
            pool.ShutDown();
            fastPathPool.ShutDown();
        }

        /// <summary>
        /// 获取连接对象
        /// 
        /// Get the connection object
        /// </summary>
        /// <returns></returns>
        public IDbConnection GetConnection()
        {
            if (isShutdown)
            {
                throw new SQLException("HikariDataSource " + this + " has been closed.");
            }
            if(isInit)
            {
                //全局配置初始化
                // Global configuration initialization
                GlobalDBType.LoadXml(this.DBTypeXml);
                //
                if(!string.IsNullOrEmpty(this.DBType))
                {
                    //根据全局配置信息查找DLL
                    // Find DLL based on global configuration information
                    var dllinfo = GlobalDBType.GetDriver(this.DBType);
                    if (dllinfo != null)
                    {
                        if (string.IsNullOrEmpty(this.DriverDLLFile))
                        {
                            this.DriverDLLFile = dllinfo.DriverDLLName;
                        }
                    }
                }
                isInit = false;
            }
            if (fastPathPool != null)
            {
                return fastPathPool.GetConnection();
            }
            HikariPool result = pool;
            if (result == null)
            {
                lock (this)
                {
                    result = pool;
                    if (result == null)
                    {
                        Validate();
                        Logger.Singleton.InfoFormat("{} - Starting...", PoolName);
                        try
                        {
                            pool = result = new HikariPool(this);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            Logger.Singleton.Error(ex.Message);
                        }
                        Logger.Singleton.InfoFormat("{} - Start completed.", PoolName);
                    }
                }
            }

            return result.GetConnection();
        }

        /// <summary>
        /// 关闭
        /// 
        /// closure
        /// </summary>
        public void Close()
        {
            if (isShutdown)
            {
                return;
            }

            HikariPool p = pool;
            if (p != null)
            {
                try
                {
                    Logger.Singleton.InfoFormat("{0} - Shutdown initiated...", PoolName);
                    p.ShutDown();
                    Logger.Singleton.InfoFormat("{0} - Shutdown completed.", PoolName);
                }
                catch (Exception e)
                {
                    Logger.Singleton.WarnFormat("{0} - Interrupted during closing,errormsg:{1}", PoolName,e);
                    Thread.CurrentThread.Interrupt();
                }
            }
        }

        /// <summary>
        /// 不允许使用该方法
        /// </summary>
        /// <param name="filePath"></param>
        public override void LoadConfig(string filePath)
        {
            //throw new Exception("不允许使用该方法");
            throw new Exception("This method is not allowed");
        }


        /// <summary>
        /// 获取Bulk处理接口对象
        /// </summary>
        /// <returns></returns>
        public IBulkCopy GetBulkCopy()
        {
            HikariConnection con =(HikariConnection)GetConnection();
            var cls = pool.GetBulkCopy();
            return  new DBBulkCopy() { BulkCls = cls, Connection=con};
        }
       

       
        #region ADO.NET对象

        public IDbDataAdapter DataAdapter { get {return pool.GetDataAdapter(); } }

        public IDbCommand DbCommand { get { return pool.GetDbCommand(); } }

        public IDbDataParameter DataParameter { get { return pool.GetDataParameter(); } }

        #endregion

        /// <summary>
        /// 验证SQL
        /// 
        /// Verify SQL
        /// </summary>
        /// <returns></returns>
        public bool CheckSQL()
        {
            using (var con = GetConnection())
            {
                try
                {
                    if (string.IsNullOrEmpty(this.ConnectionInitSql))
                    {
                        var cmd = con.CreateCommand();
                        cmd.CommandText = ConnectionInitSql;
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                    }
                    return true;
                }
                catch(Exception ex)
                {
                    //Logger.Singleton.Info("验证SQL有异常,异常信息:"+ex.Message);
                    Logger.Singleton.Info("Verify that the SQL is abnormal, abnormal information:" + ex.Message);
                    return false;
                }
            }
        }

        public override string ToString()
        {
            return "HikariDataSource (" + pool + ")";
        }
    }
}
