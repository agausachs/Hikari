﻿using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

/**
* 命名空间: Hikari 
* 类 名： HikariPool
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace Hikari
{
    //  public delegate void BagEntryRemove<T>(object sender, T[] entrys);
    /// <summary>
    /// 功能描述    ：HikariPool  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/10/24 16:25:50 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/10/24 16:25:50 
    /// </summary>
    internal class HikariPool:PoolBase
    {
        
        public static  int POOL_NORMAL = 0; //正常 // normal
        public static  int POOL_SUSPENDED = 1; //挂起 // hang
        public static  int POOL_SHUTDOWN = 2; //关闭 //closure
        public volatile int poolState=0; //当前状态 // Current state
        private AutoResetEvent resetEvent = null;
        private readonly object lock_obj = new object();
        private volatile bool isWaitAdd = true; //快速添加 // Quickly add
        private int logNumTime = 0;
        public string ConnectStr { get; set; }

        /// <summary>
        /// 
        /// tick与毫秒的转化值
        /// 
        /// Conversion value of tick and milliseconds
        /// 
        /// </summary>
        private const int TicksMs = 10000;
        private KeepingExecutorService keepingExecutor;
        private ConnectionBucket<PoolEntry> connectionBag=null;

        
        public HikariPool(HikariDataSource hikariDataSource) : base(hikariDataSource)
        {
            connectionBag = new ConnectionBucket<PoolEntry>();
            keepingExecutor = new KeepingExecutorService(hikariDataSource.IdleTimeout, hikariDataSource.MaxLifetime, hikariDataSource.LeakDetectionThreshold);
            resetEvent = new AutoResetEvent(true);
            CheckFailFast();//初始化创建 // Initial creation
            connectionBag.ArrayEntryRemove += ConnectionBag_ArrayEntryRemove;
            this.logNumTime = hikariDataSource.LogNumberTime;
            LogPoolNumber();
            ConnectStr = config.ConnectString;
        }

        /// <summary>
        /// 标记移除的进行处理
        /// 
        /// Mark removal for processing
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="entrys"></param>
        private void ConnectionBag_ArrayEntryRemove(object sender, PoolEntry[] entrys)
        {
          if(entrys!=null&&entrys.Length>0)
            {
                foreach(PoolEntry entry in entrys)
                {
                    CloseConnection(entry.Close());
                }
            }
        }

        /// <summary>
        /// 释放使用
        /// 
        /// Release to use
        /// </summary>
        /// <param name="poolEntry"></param>
        internal void Recycle(PoolEntry poolEntry)
        {
            if(poolEntry.State==IConcurrentBagEntry.STATE_REMOVED)
            {
                //已经标记移除的不能再加入，集合也不允许
                // Those that have been marked for removal can no longer be added, and collections are not allowed
                CloseConnection(poolEntry.Close());
                return;
            }
            if(!connectionBag.Push(poolEntry))
            {
                CloseConnection(poolEntry.Close());
            }
            else
            {
                //监视空闲
                // Watch for idle
                keepingExecutor.ScheduleIdleTimeout(poolEntry);
            }
        }

        /// <summary>
        /// 获取连接
        /// 
        /// Get connection
        /// </summary>
        /// <returns></returns>
        internal IDbConnection GetConnection()
        {
            return GetConnection(connectionTimeout);
        }

        /// <summary>
        /// 超时获取
        /// 
        /// Timed out
        /// </summary>
        /// <param name="hardTimeout">毫秒</param>
        /// <returns></returns>
        public IDbConnection GetConnection(long hardTimeout)
        {
            if(poolState== POOL_SHUTDOWN)
            {
                return null;
            }
            if(poolState==POOL_SUSPENDED)
            {
                //挂起操作
                // Suspend operation
                resetEvent.WaitOne();
            }
         
            long startTime = DateTime.Now.Ticks;
            try
            {
                long timeout = hardTimeout;
                do
                {
                    PoolEntry poolEntry = null;
                    if(connectionBag.TryPop(out poolEntry))
                    {
                        try
                        {
                            if (poolEntry.State == IConcurrentBagEntry.STATE_REMOVED)
                            {
                                //已经要移除的
                                // Already removed
                                CloseConnection(poolEntry.Close());
                                continue;//继续获取 // Keep getting
                            }
                            keepingExecutor.ScheduleUse(poolEntry);
                        }
                        catch (Exception ex)
                        {
                            //throw new Exception("获取失败:" + ex.Message);
                            throw new Exception("Get failed:" + ex.Message);
                        }
                        //每次产生代理连接，代理连接在外面会关闭，返回连接池的对象
                        // Each time a proxy connection is generated, the proxy connection will be closed outside, and the object of the connection pool will be returned
                        return poolEntry.CreateProxyConnection(DateTime.Now.Ticks);
                    }
                    else
                    {
                        CheckPool();//监测连接 // Monitor connection
                        if (size < config.MaximumPoolSize)
                        {
                            //创建新的,不再进入集合
                            // Create a new one, no longer enter the collection
                            poolEntry = CreatePoolEntry();
                            if (poolEntry != null)
                            {
                                poolEntry.CompareAndSetState(IConcurrentBagEntry.STATE_NOT_IN_USE, IConcurrentBagEntry.STATE_IN_USE);
                                keepingExecutor.ScheduleUse(poolEntry);
                                return poolEntry.CreateProxyConnection(DateTime.Now.Ticks);
                            }
                          
                        }
                    }
                    //计算获取的时间，转化成ms
                    // Calculate the acquired time and convert it into ms
                    timeout = timeout- (DateTime.Now.Ticks - startTime) / TicksMs;
                } while (timeout > 0L);
            }
            catch (Exception e)
            {
                throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
            }
            //throw new SQLException(poolName + " 无法获取连接对象,需要检测网络或者数据库服务");
            throw new SQLException(poolName + " Unable to get the connection object, need to check the network or database service");
        }

        /// <summary>
        /// 初始化创建
        /// </summary>
        private void CheckFailFast()
        {
            long initializationTimeout = config.InitializationFailTimeout;
            if (initializationTimeout < 0)
            {
                return;
            }

            long startTime = DateTime.Now.Ticks;
            do
            {
                PoolEntry poolEntry = CreatePoolEntry();
                if (poolEntry != null)
                {
                    if (config.MinimumIdle > 0)
                    {
                        connectionBag.Push(poolEntry);
                      
                        Logger.Singleton.DebugFormat("{0} - Added connection {1}", poolName, poolEntry);
                    }
                }
                startTime = (DateTime.Now.Ticks - startTime) / TicksMs;
            } while (startTime < initializationTimeout);

           
        }


        /// <summary>
        /// 创建池中对象
        /// </summary>
        /// <returns></returns>
        private PoolEntry CreatePoolEntry()
        {
            try
            {
                PoolEntry poolEntry = NewPoolEntry();
                if(poolEntry==null)
                {
                    return null;
                }
                long maxLifetime = config.MaxLifetime;
                if (maxLifetime > 0&&poolEntry!=null)
                {
                    // variance up to 2.5% of the maxlifetime
                    Random random = new Random();
                    long variance = maxLifetime > 10_000 ? random.Next((int)maxLifetime / 40) : 0;
                    long lifetime = maxLifetime - variance;
                    keepingExecutor.ScheduleMaxLive(poolEntry);
                }
              
                return poolEntry;
            }
            catch (SQLException e)
            {
               
                if (poolState == POOL_NORMAL)
                { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
                    Logger.Singleton.DebugFormat("{0} - Cannot acquire connection from data source", poolName);
                }
                Logger.Singleton.DebugFormat("{0} - Cannot acquire connection from data source,error:{1}", poolName,e);
                return null;
            }
            catch (Exception e)
            {
                if (poolState == POOL_NORMAL)
                { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
                    Logger.Singleton.ErrorFormat("{0} - Error thrown while acquiring connection from data source,{1}", poolName,e.Message);

                }
                return null;
            }
        }

        /// <summary>
        /// 开启线程监测连接
        /// 
        /// Open thread monitoring connection
        /// </summary>
        private void CheckPool()
        {
            if (config.MinimumIdle == 0)
            {
                config.MinimumIdle = config.MaximumPoolSize;
                if (config.MinimumIdle > 2 * Environment.ProcessorCount)
                {
                    config.MinimumIdle = Environment.ProcessorCount * 2;
                }
            }
            //isWaitAdd 
            if (isWaitAdd)
            {
                isWaitAdd = false;
                Task.Factory.StartNew((Action)(() =>
                {
                    int num = 10;//20s内的监测 // Monitoring within 20s
                    while (true)
                    {
                        //挂满池中最小空闲
                        // The smallest idle in the full pool
                        while (size < config.MaximumPoolSize && connectionBag.Count < config.MinimumIdle)
                        {
                            //迅速添加连接爬升，可以从池中获取
                            // Quickly add connection climbs, which can be obtained from the pool
                            PoolEntry poolEntry = CreatePoolEntry();
                            if (poolEntry != null)
                            {
                                connectionBag.Push(poolEntry);
                            }
                        }
                        Thread.Sleep(2000);//延迟2秒，监测 // Delay 2 seconds, monitor
                        num--;
                        if(num == 0)
                        {
                            break;
                        }
                    }
                    isWaitAdd = true;
                  
                }));
            }
        }


        /// <summary>
        /// 清理所有存在的连接
        /// 然后重新创建
        /// 
        /// Clean up all existing connections
        /// Then recreate
        /// </summary>
        public void Clear()
        {
            poolState = POOL_SUSPENDED;
            PoolEntry poolEntry = null;
            //清理资源
            // Clean up resources
            while (true)
            {
                if (connectionBag.TryPop(out poolEntry))
                {
                    CloseConnection(poolEntry.Close());
                }
                else if (connectionBag.IsEmpty)
                {
                    break;
                }
                else
                {
                    continue;
                }
            }
            size = 0;
            //
            keepingExecutor.Clear();//清除 // clear
            poolState = POOL_NORMAL;
            resetEvent.Set();//恢复等待； // resume waiting;
        }

        /// <summary>
        /// 关闭池，不能再使用
        /// 
        /// Close the pool and can no longer use it
        /// </summary>
        public void ShutDown()
        {
            lock (lock_obj)
            {
                try
                {
                    poolState = POOL_SHUTDOWN;
                    PoolEntry poolEntry = null;

                    //清理资源
                    // Clean up resources
                    while (true)
                    {
                        if (connectionBag.TryPop(out poolEntry))
                        {
                            CloseConnection(poolEntry.Close());
                        }
                        else if (connectionBag.IsEmpty)
                        {
                            break;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    size = 0;
                    keepingExecutor.Stop();//清除所有资源 // Clear all resources
                    LogPoolState("Before shutdown ");
                }
                finally
                {
                    LogPoolState("After shutdown ");
                }
            }
        }


        /// <summary>
        /// 输出线程池状态
        /// DEBUG日志
        /// 
        /// Output thread pool status
        /// DEBUG log
        /// </summary>
        /// <param name="v"></param>
        private void LogPoolState(params string[] v)
        {
                Logger.Singleton.DebugFormat("{0} - {1}stats (total={2}, active={3}, idle={4}, waiting={5})",
                             poolName,poolState, (v.Length > 0 ? v[0] : ""),
                             connectionBag.Count, 0, 0);
        }


        /// <summary>
        /// 输出DEBUG日志，显示池中数据量
        /// 
        /// Output DEBUG log, showing the amount of data in the pool
        /// </summary>
        private void LogPoolNumber()
        {
            if(logNumTime<=0)
            {
                return;
            }
            Task.Factory.StartNew(() =>
            {
                //分钟缓存毫秒
                // Cache milliseconds in minutes
                Thread.Sleep(logNumTime *60* 1000);
                Logger.Singleton.DebugFormat("PoolConnection {0} - bag:{1}-total:{2})",
                          poolName,
                          connectionBag.Count,size);
                LogPoolNumber();//递归线程 // recursive thread
            });
          
        }
    }
}
