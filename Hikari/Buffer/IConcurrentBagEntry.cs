using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Hikari
{
    public delegate void BagEntryUpdateState(object sender, int state);
   public abstract class IConcurrentBagEntry
    {
        public const int STATE_NOT_IN_USE = 0;//没有使用 // not used
        public const int STATE_IN_USE = 1;//正在使用 // In use
        public const int STATE_REMOVED = -1;//移除 // Remove
        public const int STATE_RESERVED = -2;//预留 // Reserved
        protected   int state;//使用状态 // Usage status
        public event BagEntryUpdateState StateUpdate;

        /// <summary>
        /// 修改状态
        /// 
        /// Modify status
        /// </summary>
        /// <param name="expectState">The current state</param>
        /// <param name="newState">New state</param>
        /// <returns></returns>
        public virtual bool CompareAndSetState(int expectState, int newState)
        {
            if (Interlocked.CompareExchange(ref state, newState, expectState) != expectState)
            {
                if(StateUpdate!=null)
                {
                    StateUpdate(this, newState);
                }
                return true;
            }
            else
            {
                //没有替换
                return false;
            }
        }

        /// <summary>
        /// 直接设置状态
        /// 
        /// Set the status directly
        /// </summary>
        /// <param name="newState"></param>
        public virtual void SetState( int newState)
        {
            Interlocked.Exchange(ref state, newState);
        }

        /// <summary>
        /// 注册状态信息
        /// 
        /// Registration status information
        /// </summary>
        public bool IsRegister { get { return StateUpdate != null; } }


        /// <summary>
        /// 状态
        /// 
        /// state
        /// </summary>
        public int State { get { return state; } }
    }
}
