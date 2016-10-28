using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Numerics;
using System.Reflection;

namespace AsyncDataAdapter
{
    public static class Helpers
    {
        private static readonly MethodInfo AdapterInitInt = typeof(RowUpdatedEventArgs).GetMethod("AdapterInit", BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { typeof(int) }, null);
        private static readonly MethodInfo AdapterInitDataRows = typeof(RowUpdatedEventArgs).GetMethod("AdapterInit", BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { typeof(DataRow[]) }, null);
        private static readonly MethodInfo EnsureAdditionalCapacityMethod = typeof(DataColumnCollection).GetMethod("EnsureAdditionalCapacity", BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { typeof(int) }, null);
        private static readonly PropertyInfo getter = typeof(RowUpdatedEventArgs).GetProperty("Rows", BindingFlags.NonPublic | BindingFlags.Instance);

        public static void AdapterInit(this RowUpdatedEventArgs e, int rowCount)
        {
            AdapterInitInt.Invoke(e, new object[] {rowCount});
        }

        public static void AdapterInit(this RowUpdatedEventArgs e, DataRow[] rowBatch)
        {
            AdapterInitDataRows.Invoke(e, new object[] { rowBatch });
        }

        public static DataRow[] GetRows(this RowUpdatedEventArgs e)
        {
            DataRow[] rows = (DataRow[]) getter.GetMethod.Invoke(e, null);
            
            return rows;
        }

        public static DataRow  GetRows(this RowUpdatedEventArgs e, int row)
        {
            return e.GetRows()[row];
        }

        public static void EnsureAdditionalCapacity(this DataColumnCollection c, int capacity)
        {
            EnsureAdditionalCapacityMethod.Invoke(c, new object[] {capacity});
        }   

        internal static bool IsAutoIncrementType(Type dataType)
        {
            if (!(dataType == typeof(int)) && !(dataType == typeof(long)) && (!(dataType == typeof(short)) && !(dataType == typeof(Decimal))) && (!(dataType == typeof(BigInteger)) && !(dataType == typeof(SqlInt32)) && (!(dataType == typeof(SqlInt64)) && !(dataType == typeof(SqlInt16)))))
                return dataType == typeof(SqlDecimal);
            return true;
        }

        internal static DataColumn CreateDataColumnBySchemaAction(string sourceColumn, string dataSetColumn, DataTable dataTable, Type dataType, MissingSchemaAction schemaAction)
        {
            if (ADP.IsEmpty(dataSetColumn))
                return (DataColumn)null;
            switch (schemaAction)
            {
                case MissingSchemaAction.Add:
                case MissingSchemaAction.AddWithKey:
                    return new DataColumn(dataSetColumn, dataType);
                case MissingSchemaAction.Ignore:
                    return (DataColumn)null;
                case MissingSchemaAction.Error:
                    throw ADP.ColumnSchemaMissing(dataSetColumn, dataTable.TableName, sourceColumn);
                default:
                    throw ADP.InvalidMissingSchemaAction(schemaAction);
            }
        }
    }
}