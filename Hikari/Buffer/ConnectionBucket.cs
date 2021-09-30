using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Hikari
{
    /// <summary>
    /// 移除关闭委托
    /// 
    /// Remove and close the commission
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="sender"></param>
    /// <param name="entrys"></param>
    public delegate void BagEntryRemove<T>(object sender, T[] entrys);

    /// <summary>
    /// 连接池数据区
    /// 
    /// Connection pool data area
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ConnectionBucket<T> where T : IConcurrentBagEntry
    {
        private ConcurrentStack<T> concurrentStack = null;
        private DateTime emptyTime = DateTime.Now;
        private int emptyTimeM = 2 * 60;//监测 // Monitoring

        /// <summary>
        /// 已经移除的对象
        /// 
        /// Objects that have been removed
        /// </summary>
        public event BagEntryRemove<T> ArrayEntryRemove = null;


        /// <summary>
        /// 是否为空
        /// 
        /// Is it empty?
        /// </summary>
        public bool IsEmpty { get { return concurrentStack.IsEmpty; } }

        /// <summary>
        /// 缓存量
        /// 
        /// Cache amount
        /// </summary>
        public int Count { get { return concurrentStack.Count; } }

        /// <summary>
        /// 监视空闲缓存时间长度
        /// 该时间段内没有出现空闲的情况，则清除现有缓存的数据
        /// 单位：分
        /// 默认:120分钟
        /// 
        /// Monitor the length of the idle buffer time
        /// If there is no idle situation in this time period, then clear the existing cached data
        /// Unit: minutes
        /// Default: 120 minutes
        /// </summary>
        public int EmptyTime { get { return emptyTimeM; } set { emptyTimeM = value; } }

        public ConnectionBucket()
        {
            concurrentStack = new ConcurrentStack<T>();

        }

        /// <summary>
        /// 构造方法
        /// 该功能不需要，无法初始化独立创建
        /// 
        /// Construction method
        /// This function is not needed and cannot be initialized and created independently
        /// </summary>
        /// <param name="capticty">Initial capacity</param>
        private ConnectionBucket(int capticty)
        {
            concurrentStack = new ConcurrentStack<T>();

            T[] array = (T[])Array.CreateInstance(typeof(T), capticty);
            if (array != null)
            {
                PushRange(array);
            }
        }

        /// <summary>
        /// 添加移除的数据
        /// 
        /// Add and remove data
        /// </summary>
        /// <param name="item"></param>
        private void AddRemove(T item)
        {
            if (ArrayEntryRemove != null)
            {

                Task.Factory.StartNew(() =>
                {
                    ArrayEntryRemove(this, new T[] { item });
                });
            }

        }

        /// <summary>
        /// 堆栈出顶
        /// 
        /// Stack out
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryPop(out T item)
        {
            item = null;
            while (true)
            {
                if (!concurrentStack.TryPop(out item))
                {
                    if (concurrentStack.IsEmpty)
                    {
                        emptyTime = DateTime.Now;
                        break;
                    }
                }
                if (item.State == IConcurrentBagEntry.STATE_REMOVED)
                {
                    AddRemove(item);//将获取时加入 // // will be added when getting

                }
                else
                {
                    break;
                }
            }
            if (item != null)
            {
                //设置状态;只有使用的更新
                //其它例如删除的不需要
                // Set status; only used updates
                // Others such as deletion are not needed
                item.CompareAndSetState(IConcurrentBagEntry.STATE_NOT_IN_USE, IConcurrentBagEntry.STATE_IN_USE);
                return true;
            }
            return false;

        }

        /// <summary>
        /// 堆栈出顶，不移除
        /// 
        /// The stack is out of the top, not removed
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryPeek(out T item)
        {
            item = null;
            while (true)
            {
                if (!concurrentStack.TryPeek(out item))
                {
                    if (concurrentStack.IsEmpty)
                    {
                        break;
                    }
                }
                if (item.State == IConcurrentBagEntry.STATE_REMOVED)
                {
                    concurrentStack.TryPop(out item);
                    AddRemove(item);//将获取时加入 // will be added when getting

                }
                else
                {
                    break;
                }
            }
            if (item != null)
            {
                item.CompareAndSetState(IConcurrentBagEntry.STATE_NOT_IN_USE, IConcurrentBagEntry.STATE_IN_USE);
                return true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// 不允许添加已经标记移除的
        /// 堆栈顶添加元素
        /// 
        /// It is not allowed to add the ones that have been marked for removal
        /// Add an element to the top of the stack
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool Push(T item)
        {
            if (item == null || item.State == IConcurrentBagEntry.STATE_REMOVED)
            {
                return false;
            }
            else
            {

                Check();//放入时监测，不影响业务 // Monitoring when put in, does not affect business
                item.CompareAndSetState(IConcurrentBagEntry.STATE_IN_USE, IConcurrentBagEntry.STATE_NOT_IN_USE);
                concurrentStack.Push(item);
                return true;
            }
        }

        /// <summary>
        /// 添加一组元素
        /// 其中已经标记移除的不能加入
        /// 
        /// Add a set of elements
        /// Those that have been marked for removal cannot be added
        /// </summary>
        /// <param name="items"></param>
        public void PushRange(T[] items)
        {
            foreach (T item in items)
            {
                Push(item);
            }
        }

        /// <summary>
        /// 移除,修改状态
        /// 
        /// Remove, modify state
        /// </summary>
        /// <param name="item"></param>
        public void Remove(T item)
        {
            //修改状态即可
            item.CompareAndSetState(IConcurrentBagEntry.STATE_NOT_IN_USE, IConcurrentBagEntry.STATE_REMOVED);
        }

        /// <summary>
        /// 监测空闲情况
        /// 
        /// Monitor idle conditions
        /// </summary>
        private bool Check()
        {
            if ((DateTime.Now - emptyTime).TotalMinutes > emptyTimeM)
            {
                //说明超过emptyTimeM分钟都没有用完缓存
                //准备自动移除现有缓存，重新建立新的缓存
                //将现有缓存全部视为空闲的
                // It means that the cache has not been used up in more than emptyTimeM minutes
                // Prepare to automatically remove the existing cache and re-create a new cache
                // Treat all existing caches as free
                T item = null;
                while (!concurrentStack.IsEmpty)
                {
                    if (concurrentStack.TryPop(out item))
                    {
                        item.SetState(IConcurrentBagEntry.STATE_REMOVED);
                        AddRemove(item);
                    }
                }
                return false;
            }
            return true;
        }
    }
}
