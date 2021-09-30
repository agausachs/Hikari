using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Hikari
{

    /// <summary>
    /// 创建驱动连接
    /// 
    /// Create driver connection
    /// </summary>
    public abstract class PoolBase
    {
      
        protected static int MAX_PERMITS = 10000;
        protected HikariConfig config;
        protected string poolName;
        protected long connectionTimeout;
        protected int validationTimeout;
        protected string dllPath = ""; //dll路径 // dll path
        protected  int size = 0; //生成的连接数量 // Number of connections generated
        protected int entryid = 0; //ID生成 // ID generation

        /// <summary>
        /// 已经创建的数据
        /// 
        /// Data that has been created
        /// </summary>
        public int Size { get { return size; } }

        public PoolBase(HikariConfig config)
        {
            this.config = config;
            this.poolName = config.PoolName;
            this.connectionTimeout = config.ConnectionTimeout;
            this.validationTimeout =(int) config.ValidationTimeout;
        }


        /// <summary>
        /// 创建池中数据对象
        /// 
        /// Create a data object in the pool
        /// </summary>
        /// <returns></returns>
        protected PoolEntry NewPoolEntry()
        {
            PoolEntry poolEntry= new PoolEntry(NewConnection(), this);
            if (poolEntry.IsValidate)
            {
                //创建无效
                // create invalid
                poolEntry = null;
            }
            if(poolEntry!=null)
            {
                poolEntry.ID=Interlocked.Increment(ref entryid);
                Interlocked.Increment(ref size);
            }
            return poolEntry;
        }

        /// <summary>
        /// 关闭驱动连接
        /// 按照设计，只有连接池能够操作驱动连接
        /// 
        /// Close the drive connection
        /// According to the design, only the connection pool can operate the driver connection
        /// </summary>
        /// <param name="connection"></param>
        protected void CloseConnection(IDbConnection connection)
        {
            if(connection!=null)
            {
                connection.Close();
                connection.Dispose();
                Interlocked.Decrement(ref size);
            }
        }

        /// <summary>
        /// 创建驱动的连接
        /// 
        /// Create a driver connection
        /// </summary>
        /// <returns></returns>
        private IDbConnection NewConnection()
        {
            long start = DateTime.Now.Ticks;
            IDbConnection connection = null;
            try
            {

                if (string.IsNullOrEmpty(dllPath))
                {
                    if (config.DriverDir == null)
                    {
                        throw new Exception("config DriverDir null unexpectedly");
                    }
                    if (config.DriverDLLFile == null)
                    {
                        throw new Exception("config DriverDLLFile null unexpectedly");
                    }
                    dllPath = Path.Combine(config.DriverDir, config.DriverDLLFile);
                }
                connection = DbProviderFactories.GetConnection(dllPath);
                if (connection == null)
                {
                    throw new Exception("DataSource returned null unexpectedly");
                }
                SetupConnection(connection);
                if (connection == null)
                {
                    throw new Exception("Open Connection returned null unexpectedly");
                }
                if(connection.State!=ConnectionState.Open)
                {
                    connection.Dispose();
                    connection = null;
                    return null;
                }
                return connection;
            }
            catch (Exception e)
            {
                
                throw e;
            }
           
        }

        /// <summary>
        /// 测试连接及设置
        /// 
        /// Test connection and settings
        /// </summary>
        /// <param name="connection"></param>
        private void SetupConnection(IDbConnection connection)
        {
            try
            {
                connection.ConnectionString = config.ConnectString;
                ExecuteSql(connection, config.ConnectionInitSql, true);
            }
            catch (SQLException e)
            {
                throw new Exception(e.Message);
            }
        }

        /// <summary>
        /// 连接验证
        /// 
        /// Connection verification
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="v"></param>
        private void ExecuteSql(IDbConnection connection, string sql, bool v)
        {
            var task= Task.Factory.StartNew(() =>
            {
                try
                {
                    connection.Open();
                }
                catch
                {
                    return;
                }
            }
            );
            if(!task.Wait((int)config.ConnectionTimeout))
            {
                HealthCheckRegistry.Singleton.Add(poolName, this);
                //Logger.Singleton.Warn("数据库连接异常，连接池：" + poolName);
                Logger.Singleton.Warn("Abnormal database connection, connection pool：" + poolName);
                return;
            }
         
            if (string.IsNullOrEmpty(sql))
            {
                return;
            }
            var cts = new CancellationTokenSource(validationTimeout);
            //var cancell = cts.Token.Register(() => Logger.Singleton.Warn("当前连接执行测试超时,数据库异常，SQL:" + sql));
            var cancell = cts.Token.Register(() => Logger.Singleton.Warn("The current connection execution test timed out, the database is abnormal, SQL:" + sql));
            var result = Task.Factory.StartNew(() =>
             {
                 try
                 {
                     IDbCommand command = connection.CreateCommand();
                     command.CommandText = sql;
                     int r = command.ExecuteNonQuery();
                     command.Dispose();
                 }
                 catch(Exception ex)
                 {
                     connection.Close();
                     connection.Dispose();
                     connection = null;
                     //Logger.Singleton.Error("执行验证SQL失败,连接关闭!原因："+ex.Message);
                     Logger.Singleton.Error("The execution of the verification SQL failed and the connection was closed! Reason:" + ex.Message);
                 }

             }, cts.Token
                 );
            cancell.Dispose();
            cts.Dispose();

        }

     
        public override string ToString()
        {
            return poolName;
        }


        #region 数据库主要对象 The main object of the database
        public IDbCommand  GetDbCommand()
        {
            return DbProviderFactories.GetDbCommand(dllPath);
        }

        public IDbDataParameter GetDataParameter()
        {
            return DbProviderFactories.GetDataParameter(dllPath);
        }

        public IDbDataAdapter GetDataAdapter()
        {
            return DbProviderFactories.GetDataAdapter(dllPath);
        }

        public Type GetBulkCopy()
        {
            return DbProviderFactories.GetBulkCopyClass(dllPath);
        }
        #endregion
    }
}