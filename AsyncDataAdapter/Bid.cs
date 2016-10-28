using System;
using System.Data;

namespace AsyncDataAdapter
{
    public static class Bid
    {
        public static void Trace(string format, params object[] values)
        {
            
        }

        public static void ScopeEnter(out IntPtr hscp, string format, params object[] values)
        {
            hscp = IntPtr.Zero;
        }

        public static void ScopeLeave(ref IntPtr hscp)
        {
            
        }
    }
}