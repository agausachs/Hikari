﻿using System;
using System.Data;
using System.Threading;

namespace Hikari
{


    /// <summary>
    /// 连接池缓存对象
    /// 包含驱动连接
    /// 
    /// Connection pool cache object
    /// Contains driver connection
    /// </summary>
    public class PoolEntry : IConcurrentBagEntry
    {

        /// <summary>
        /// 驱动连接
        /// 
        /// Driver connection
        /// </summary>
        private readonly IDbConnection connection;

        private long createTime = 0;
        private long lastAccessed = 0;

        private readonly HikariPool hikariPool;//所属的连接池 // Belonging to the connection pool

        /// <summary>
        /// 实体ID,没有实际意义
        /// 当前主要是方便调试
        /// 
        /// Entity ID, has no practical meaning
        /// At present, it is mainly to facilitate debugging
        /// </summary>
        public int ID { get; set; }


        /// <summary>
        /// 操作时间
        /// 
        /// Operation time
        /// </summary>
        public long AccessedTime { get { return lastAccessed; } set { lastAccessed = value; } }

        /// <summary>
        /// 创建时间
        /// 
        /// Creation time
        /// </summary>
        public long CreateTime { get { return createTime; } }

        /// <summary>
        /// 连接是否有效
        /// 
        /// Is the connection valid
        /// </summary>
        public bool IsValidate { get { return connection == null; } }

        /// <summary>
        /// 构造
        /// 
        /// Construction
        /// </summary>
        /// <param name="connection">Drive connection</param>
        /// <param name="pool">Pool</param>
        public PoolEntry(IDbConnection connection, PoolBase pool)
        {
            this.connection = connection;
            this.hikariPool = (HikariPool)pool;
            this.lastAccessed = DateTime.Now.Ticks;
            this.createTime = lastAccessed;
        }


        /// <summary>
        /// 创建代理连接
        /// 将驱动包到代理连接中
        /// 
        /// Create proxy connection
        /// Put the driver package into the proxy connection
        /// </summary>
        /// <param name="now"></param>
        /// <returns></returns>
        public IDbConnection CreateProxyConnection(long now)
        {
            try
            {
                this.AccessedTime = DateTime.Now.Ticks;
                return ProxyFactory.GetProxyConnection(this, connection, now);
            }
            catch (Exception ex)
            {
                //throw new Exception("获取失败:" + ex.Message);
                throw new Exception("Get failed:" + ex.Message);
            }
        }

        public void ResetConnectionState(ProxyConnection proxyConnection, int dirtyBits)
        {
            // hikariPool.ResetConnectionState(connection, proxyConnection, dirtyBits);
        }


        /// <summary>
        /// 释放池中
        /// 
        /// Release the pool
        /// </summary>
        /// <param name="lastAccess"></param>
        public void Recycle(long lastAccess)
        {
            if (connection != null)
            {
                this.lastAccessed = lastAccess;
                hikariPool.Recycle(this);
            }
            if(connection.State!=ConnectionState.Open)
            {
                this.Close();
            }
        }

        /// <summary>
        /// 连接信息
        /// 
        /// Connection information
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            _ = DateTime.Now.Ticks;
            return connection
               + ", accessed " + " ago, "
               + StateToString();
        }

        // ***********************************************************************
        //                      IBagEntry methods
        // ***********************************************************************

        /// <summary>
        /// 返回驱动连接
        /// 
        /// Back to drive connection
        /// </summary>
        /// <returns></returns>
        public IDbConnection Close()
        {
            //防止多线程同步
            // Prevent multi-thread synchronization
            Interlocked.Exchange(ref state, IConcurrentBagEntry.STATE_REMOVED);
            this.lastAccessed = 0;
            this.createTime = 0;//以防其它地方判断使用 // in case other places judge the use
            return connection;
        }

        /// <summary>
        /// 状态描述
        /// 
        /// status description
        /// </summary>
        /// <returns></returns>
        private string StateToString()
        {
            switch (state)
            {
                case STATE_IN_USE:
                    return "IN_USE";
                case STATE_NOT_IN_USE:
                    return "NOT_IN_USE";
                case STATE_REMOVED:
                    return "REMOVED";
                case STATE_RESERVED:
                    return "RESERVED";
                default:
                    return "Invalid";
            }
        }
    }
}