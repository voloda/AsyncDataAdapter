//------------------------------------------------------------------------------
// <copyright file="DataAdapter.cs" company="Microsoft">
//      Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
// <owner current="true" primary="true">[....]</owner>
// <owner current="true" primary="false">[....]</owner>
//------------------------------------------------------------------------------

using System.Data.Common;
using System.Threading.Tasks;

namespace AsyncDataAdapter
{

    using System;
    using System.ComponentModel;
    using System.Data;
    using System.Data.ProviderBase;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;

    public class DataAdapter : Component /*, IDataAdapter */
    { // V1.0.3300

        static private readonly object EventFillError = new object();

        private bool _acceptChangesDuringUpdate = true;
        private bool _acceptChangesDuringUpdateAfterInsert = true;
        private bool _continueUpdateOnError = false;
        private bool _hasFillErrorHandler = false;
        private bool _returnProviderSpecificTypes = false;

        private bool _acceptChangesDuringFill = true;
        private LoadOption _fillLoadOption;

        private MissingMappingAction _missingMappingAction = System.Data.MissingMappingAction.Passthrough;
        private MissingSchemaAction _missingSchemaAction = System.Data.MissingSchemaAction.Add;
        private DataTableMappingCollection _tableMappings;

        private static int _objectTypeCount; // Bid counter
        internal readonly int _objectID = System.Threading.Interlocked.Increment(ref _objectTypeCount);

#if DEBUG
        // if true, we are asserting that the caller has provided a select command
        // which should not return an empty result set
        private bool _debugHookNonEmptySelectCommand = false;
#endif

        [Conditional("DEBUG")]
        void AssertReaderHandleFieldCount(DataReaderContainer readerHandler)
        {
#if DEBUG
            Debug.Assert(!_debugHookNonEmptySelectCommand || readerHandler.FieldCount > 0, "Scenario expects non-empty results but no fields reported by reader");
#endif
        }

        [Conditional("DEBUG")]
        void AssertSchemaMapping(SchemaMapping mapping)
        {
#if DEBUG
            if (_debugHookNonEmptySelectCommand)
            {
                Debug.Assert(mapping != null && mapping.DataValues != null && mapping.DataTable != null, "Debug hook specifies that non-empty results are not expected");
            }
#endif
        }

        protected DataAdapter() : base()
        { // V1.0.3300
            GC.SuppressFinalize(this);
        }

        protected DataAdapter(DataAdapter from) : base()
        { // V1.1.3300
            CloneFrom(from);
        }

        [
        DefaultValue(true),
        CategoryAttribute("Settins"),
        DescriptionAttribute("Accept changes during fill"),
        ]
        public bool AcceptChangesDuringFill
        { // V1.0.3300
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_AcceptChangesDuringFill|API> %d#\n", ObjectID);
                return _acceptChangesDuringFill;
            }
            set
            {
                _acceptChangesDuringFill = value;
                //Bid.Trace("<comm.DataAdapter.set_AcceptChangesDuringFill|API> %d#, %d\n", ObjectID, value);
            }
        }

        [
        EditorBrowsableAttribute(EditorBrowsableState.Never)
        ]
        virtual public bool ShouldSerializeAcceptChangesDuringFill()
        {
            return (0 == _fillLoadOption);
        }

        [
        DefaultValue(true),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Accept changes during update"),
        ]
        public bool AcceptChangesDuringUpdate
        {  // V1.2.3300, MDAC 74988
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_AcceptChangesDuringUpdate|API> %d#\n", ObjectID);
                return _acceptChangesDuringUpdate;
            }
            set
            {
                _acceptChangesDuringUpdate = value;
                //Bid.Trace("<comm.DataAdapter.set_AcceptChangesDuringUpdate|API> %d#, %d\n", ObjectID, value);
            }
        }

        [
        DefaultValue(false),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Continue update on error"),
        ]
        public bool ContinueUpdateOnError
        {  // V1.0.3300, MDAC 66900
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_ContinueUpdateOnError|API> %d#\n", ObjectID);
                return _continueUpdateOnError;
            }
            set
            {
                _continueUpdateOnError = value;
                //Bid.Trace("<comm.DataAdapter.set_ContinueUpdateOnError|API> %d#, %d\n", ObjectID, value);
            }
        }

        [
        RefreshProperties(RefreshProperties.All),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Fill load option"),
        ]
        public LoadOption FillLoadOption
        { // V1.2.3300
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_FillLoadOption|API> %d#\n", ObjectID);
                LoadOption fillLoadOption = _fillLoadOption;
                return ((0 != fillLoadOption) ? _fillLoadOption : LoadOption.OverwriteChanges);
            }
            set
            {
                switch (value)
                {
                    case 0: // to allow simple resetting
                    case LoadOption.OverwriteChanges:
                    case LoadOption.PreserveChanges:
                    case LoadOption.Upsert:
                        _fillLoadOption = value;
                        //Bid.Trace("<comm.DataAdapter.set_FillLoadOption|API> %d#, %d{ds.LoadOption}\n", ObjectID, (int)value);
                        break;
                    default:
                        throw ADP.InvalidLoadOption(value);
                }
            }
        }

        [
        EditorBrowsableAttribute(EditorBrowsableState.Never)
        ]
        public void ResetFillLoadOption()
        {
            _fillLoadOption = 0;
        }

        [
        EditorBrowsableAttribute(EditorBrowsableState.Never)
        ]
        virtual public bool ShouldSerializeFillLoadOption()
        {
            return (0 != _fillLoadOption);
        }

        [
        DefaultValue(System.Data.MissingMappingAction.Passthrough),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Missing mapping action"),
        ]
        public MissingMappingAction MissingMappingAction
        { // V1.0.3300
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_MissingMappingAction|API> %d#\n", ObjectID);
                return _missingMappingAction;
            }
            set
            {
                switch (value)
                { // @perfnote: Enum.IsDefined
                    case MissingMappingAction.Passthrough:
                    case MissingMappingAction.Ignore:
                    case MissingMappingAction.Error:
                        _missingMappingAction = value;
                        //Bid.Trace("<comm.DataAdapter.set_MissingMappingAction|API> %d#, %d{ds.MissingMappingAction}\n", ObjectID, (int)value);
                        break;
                    default:
                        throw ADP.InvalidMissingMappingAction(value);
                }
            }
        }

        [
        DefaultValue(MissingSchemaAction.Add),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Missing schema action"),
        ]
        public MissingSchemaAction MissingSchemaAction
        { // V1.0.3300
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_MissingSchemaAction|API> %d#\n", ObjectID);
                return _missingSchemaAction;
            }
            set
            {
                switch (value)
                { // @perfnote: Enum.IsDefined
                    case MissingSchemaAction.Add:
                    case MissingSchemaAction.Ignore:
                    case MissingSchemaAction.Error:
                    case MissingSchemaAction.AddWithKey:
                        _missingSchemaAction = value;
                        //Bid.Trace("<comm.DataAdapter.set_MissingSchemaAction|API> %d#, %d{MissingSchemaAction}\n", ObjectID, (int)value);
                        break;
                    default:
                        throw ADP.InvalidMissingSchemaAction(value);
                }
            }
        }

        internal int ObjectID
        {
            get
            {
                return _objectID;
            }
        }

        [
        DefaultValue(false),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Return provider specific types"),
        ]
        virtual public bool ReturnProviderSpecificTypes
        {
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_ReturnProviderSpecificTypes|API> %d#\n", ObjectID);
                return _returnProviderSpecificTypes;
            }
            set
            {
                _returnProviderSpecificTypes = value;
                //Bid.Trace("<comm.DataAdapter.set_ReturnProviderSpecificTypes|API> %d#, %d\n", ObjectID, (int)value);
            }
        }

        [
        DesignerSerializationVisibility(DesignerSerializationVisibility.Content),
        CategoryAttribute("Settings"),
        DescriptionAttribute("Table mappings"),
        ]
        public DataTableMappingCollection TableMappings
        { // V1.0.3300
            get
            {
                //Bid.Trace("<comm.DataAdapter.get_TableMappings|API> %d#\n", ObjectID);
                DataTableMappingCollection mappings = _tableMappings;
                if (null == mappings)
                {
                    mappings = CreateTableMappings();
                    if (null == mappings)
                    {
                        mappings = new DataTableMappingCollection();
                    }
                    _tableMappings = mappings;
                }
                return mappings; // constructed by base class
            }
        }

        //ITableMappingCollection IDataAdapter.TableMappings
        //{ // V1.0.3300
        //    get
        //    {
        //        return TableMappings;
        //    }
        //}

        virtual protected bool ShouldSerializeTableMappings()
        { // V1.0.3300, MDAC 65548
            return true; /*HasTableMappings();*/ // VS7 300569
        }

        protected bool HasTableMappings()
        { // V1.2.3300
            return ((null != _tableMappings) && (0 < TableMappings.Count));
        }

        [
        CategoryAttribute("Settings"),
        DescriptionAttribute("Fill error"),
        ]
        public event FillErrorEventHandler FillError
        { // V1.2.3300, DbDataADapter V1.0.3300
            add
            {
                _hasFillErrorHandler = true;
                Events.AddHandler(EventFillError, value);
            }
            remove
            {
                Events.RemoveHandler(EventFillError, value);
            }
        }

        [Obsolete("CloneInternals() has been deprecated.  Use the DataAdapter(DataAdapter from) constructor.  http://go.microsoft.com/fwlink/?linkid=14202")] // V1.1.3300, MDAC 81448
        [System.Security.Permissions.PermissionSetAttribute(System.Security.Permissions.SecurityAction.Demand, Name = "FullTrust")] // MDAC 82936
        virtual protected DataAdapter CloneInternals()
        { // V1.0.3300
            DataAdapter clone = (DataAdapter)Activator.CreateInstance(GetType(), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance, null, null, CultureInfo.InvariantCulture, null);
            clone.CloneFrom(this);
            return clone;
        }

        private void CloneFrom(DataAdapter from)
        {
            _acceptChangesDuringUpdate = from._acceptChangesDuringUpdate;
            _acceptChangesDuringUpdateAfterInsert = from._acceptChangesDuringUpdateAfterInsert;
            _continueUpdateOnError = from._continueUpdateOnError;
            _returnProviderSpecificTypes = from._returnProviderSpecificTypes; // WebData 101795
            _acceptChangesDuringFill = from._acceptChangesDuringFill;
            _fillLoadOption = from._fillLoadOption;
            _missingMappingAction = from._missingMappingAction;
            _missingSchemaAction = from._missingSchemaAction;

            if ((null != from._tableMappings) && (0 < from.TableMappings.Count))
            {
                DataTableMappingCollection parameters = this.TableMappings;
                foreach (object parameter in from.TableMappings)
                {
                    parameters.Add((parameter is ICloneable) ? ((ICloneable)parameter).Clone() : parameter);
                }
            }
        }

        virtual protected DataTableMappingCollection CreateTableMappings()
        { // V1.0.3300
            Bid.Trace("<comm.DataAdapter.CreateTableMappings|API> %d#\n", ObjectID);
            return new DataTableMappingCollection();
        }

        override protected void Dispose(bool disposing)
        { // V1.0.3300, MDAC 65459
            if (disposing)
            { // release mananged objects
                _tableMappings = null;
            }
            // release unmanaged objects

            base.Dispose(disposing); // notify base classes
        }

        virtual public async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType)
        { // V1.0.3300
            throw ADP.NotSupported();
        }

        virtual protected async Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, string srcTable, DbDataReader dataReader)
        { // V1.2.3300
            IntPtr hscp;
            Bid.ScopeEnter(out hscp, "<comm.DataAdapter.FillSchema|API> %d#, dataSet, schemaType=%d{ds.SchemaType}, srcTable, dataReader\n", ObjectID, (int)schemaType);
            try
            {
                if (null == dataSet)
                {
                    throw ADP.ArgumentNull("dataSet");
                }
                if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType))
                {
                    throw ADP.InvalidSchemaType(schemaType);
                }
                if (ADP.IsEmpty(srcTable))
                {
                    throw ADP.FillSchemaRequiresSourceTableName("srcTable");
                }
                if ((null == dataReader) || dataReader.IsClosed)
                {
                    throw ADP.FillRequires("dataReader");
                }
                // user must Close/Dispose of the dataReader
                object value = await Task.FromResult(FillSchemaFromReader(dataSet, null, schemaType, srcTable, dataReader));
                return (DataTable[])value;
            }
            finally
            {
                Bid.ScopeLeave(ref hscp);
            }
        }

        virtual protected async Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, DbDataReader dataReader)
        { // V1.2.3300
            IntPtr hscp;
            Bid.ScopeEnter(out hscp, "<comm.DataAdapter.FillSchema|API> %d#, dataTable, schemaType, dataReader\n", ObjectID);
            try
            {
                if (null == dataTable)
                {
                    throw ADP.ArgumentNull("dataTable");
                }
                if ((SchemaType.Source != schemaType) && (SchemaType.Mapped != schemaType))
                {
                    throw ADP.InvalidSchemaType(schemaType);
                }
                if ((null == dataReader) || dataReader.IsClosed)
                {
                    throw ADP.FillRequires("dataReader");
                }
                // user must Close/Dispose of the dataReader
                // user will have to call NextResult to access remaining results
                object value = await FillSchemaFromReader(null, dataTable, schemaType, null, dataReader);
                return (DataTable)value;
            }
            finally
            {
                Bid.ScopeLeave(ref hscp);
            }
        }

        internal async Task<object> FillSchemaFromReader(DataSet dataset, DataTable datatable, SchemaType schemaType, string srcTable, DbDataReader dataReader)
        {
            DataTable[] dataTables = null;
            int schemaCount = 0;
            do
            {
                DataReaderContainer readerHandler = DataReaderContainer.Create(dataReader, ReturnProviderSpecificTypes);

                AssertReaderHandleFieldCount(readerHandler);
                if (0 >= readerHandler.FieldCount)
                {
                    continue;
                }
                string tmp = null;
                if (null != dataset)
                {
                    tmp = DataAdapter.GetSourceTableName(srcTable, schemaCount);
                    schemaCount++; // don't increment if no SchemaTable ( a non-row returning result )
                }

                SchemaMapping mapping = new SchemaMapping(this, dataset, datatable, readerHandler, true, schemaType, tmp, false, null, null);

                if (null != datatable)
                {
                    // do not read remaining results in single DataTable case
                    return mapping.DataTable;
                }
                else if (null != mapping.DataTable)
                {
                    if (null == dataTables)
                    {
                        dataTables = new DataTable[1] { mapping.DataTable };
                    }
                    else
                    {
                        dataTables = DataAdapter.AddDataTableToArray(dataTables, mapping.DataTable);
                    }
                }
            } while (await dataReader.NextResultAsync()); // FillSchema does not capture errors for FillError event

            object value = dataTables;
            if ((null == value) && (null == datatable))
            { // WebData 101757
                value = new DataTable[0];
            }
            return value; // null if datatable had no results
        }

        virtual public async Task<int> FillAsync(DataSet dataSet)
        { // V1.0.3300
            throw ADP.NotSupported();
        }

        virtual protected async Task<int> FillAsync(DataSet dataSet, string srcTable, IDataReader dataReader, int startRecord, int maxRecords)
        { // V1.2.3300, DbDataAdapter V1.0.3300
            IntPtr hscp;
            Bid.ScopeEnter(out hscp, "<comm.DataAdapter.Fill|API> %d#, dataSet, srcTable, dataReader, startRecord, maxRecords\n", ObjectID);
            try
            {
                if (null == dataSet)
                {
                    throw ADP.FillRequires("dataSet");
                }
                if (ADP.IsEmpty(srcTable))
                {
                    throw ADP.FillRequiresSourceTableName("srcTable");
                }
                if (null == dataReader)
                {
                    throw ADP.FillRequires("dataReader");
                }
                if (startRecord < 0)
                {
                    throw ADP.InvalidStartRecord("startRecord", startRecord);
                }
                if (maxRecords < 0)
                {
                    throw ADP.InvalidMaxRecords("maxRecords", maxRecords);
                }
                if (dataReader.IsClosed)
                {
                    return 0;
                }
                // user must Close/Dispose of the dataReader
                DataReaderContainer readerHandler = DataReaderContainer.Create(dataReader, ReturnProviderSpecificTypes);
                return await FillFromReaderAsync(dataSet, null, srcTable, readerHandler, startRecord, maxRecords, null, null);
            }
            finally
            {
                Bid.ScopeLeave(ref hscp);
            }
        }

        virtual protected async Task<int> FillAsync(DataTable dataTable, IDataReader dataReader)
        { // V1.2.3300, DbDataADapter V1.0.3300
            DataTable[] dataTables = new DataTable[] { dataTable };
            return await FillAsync(dataTables, dataReader, 0, 0);
        }

        virtual protected async Task<int> FillAsync(DataTable[] dataTables, IDataReader dataReader, int startRecord, int maxRecords)
        { // V1.2.3300
            IntPtr hscp;
            Bid.ScopeEnter(out hscp, "<comm.DataAdapter.Fill|API> %d#, dataTables[], dataReader, startRecord, maxRecords\n", ObjectID);
            try
            {
                ADP.CheckArgumentLength(dataTables, "tables");
                if ((null == dataTables) || (0 == dataTables.Length) || (null == dataTables[0]))
                {
                    throw ADP.FillRequires("dataTable");
                }
                if (null == dataReader)
                {
                    throw ADP.FillRequires("dataReader");
                }
                if ((1 < dataTables.Length) && ((0 != startRecord) || (0 != maxRecords)))
                {
                    throw ADP.NotSupported(); // FillChildren is not supported with FillPage
                }

                int result = 0;
                bool enforceContraints = false;
                DataSet commonDataSet = dataTables[0].DataSet;
                try
                {
                    if (null != commonDataSet)
                    {
                        enforceContraints = commonDataSet.EnforceConstraints;
                        commonDataSet.EnforceConstraints = false;
                    }
                    for (int i = 0; i < dataTables.Length; ++i)
                    {
                        Debug.Assert(null != dataTables[i], "null DataTable Fill");

                        if (dataReader.IsClosed)
                        {
#if DEBUG
                            Debug.Assert(!_debugHookNonEmptySelectCommand, "Debug hook asserts data reader should be open");
#endif
                            break;
                        }
                        DataReaderContainer readerHandler = DataReaderContainer.Create(dataReader, ReturnProviderSpecificTypes);
                        AssertReaderHandleFieldCount(readerHandler);
                        if (readerHandler.FieldCount <= 0)
                        {
                            if (i == 0)
                            {
                                bool lastFillNextResult;
                                do
                                {
                                    lastFillNextResult = await FillNextResultAsync(readerHandler);
                                }
                                while (lastFillNextResult && readerHandler.FieldCount <= 0);
                                if (!lastFillNextResult)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                continue;
                            }
                        }
                        if ((0 < i) && !await FillNextResultAsync(readerHandler))
                        {
                            break;
                        }
                        // user must Close/Dispose of the dataReader
                        // user will have to call NextResult to access remaining results
                        int count = await FillFromReaderAsync(null, dataTables[i], null, readerHandler, startRecord, maxRecords, null, null);
                        if (0 == i)
                        {
                            result = count;
                        }
                    }
                }
                catch (ConstraintException)
                {
                    enforceContraints = false;
                    throw;
                }
                finally
                {
                    if (enforceContraints)
                    {
                        commonDataSet.EnforceConstraints = true;
                    }
                }
                return result;
            }
            finally
            {
                Bid.ScopeLeave(ref hscp);
            }
        }

        internal async Task<int> FillFromReaderAsync(DataSet dataset, DataTable datatable, string srcTable, DataReaderContainer dataReader, int startRecord, int maxRecords, DataColumn parentChapterColumn, object parentChapterValue)
        {
            int rowsAddedToDataSet = 0;
            int schemaCount = 0;
            do
            {
                AssertReaderHandleFieldCount(dataReader);
                if (0 >= dataReader.FieldCount)
                {
                    continue; // loop to next result
                }

                SchemaMapping mapping = FillMapping(dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue);
                schemaCount++; // don't increment if no SchemaTable ( a non-row returning result )

                AssertSchemaMapping(mapping);

                if (null == mapping)
                {
                    continue; // loop to next result
                }
                if (null == mapping.DataValues)
                {
                    continue; // loop to next result
                }
                if (null == mapping.DataTable)
                {
                    continue; // loop to next result
                }
                mapping.DataTable.BeginLoadData();
                try
                {
                    // startRecord and maxRecords only apply to the first resultset
                    if ((1 == schemaCount) && ((0 < startRecord) || (0 < maxRecords)))
                    {
                        rowsAddedToDataSet = await FillLoadDataRowChunkAsync(mapping, startRecord, maxRecords);
                    }
                    else
                    {
                        int count = await FillLoadDataRowAsync(mapping);

                        if (1 == schemaCount)
                        { // MDAC 71347
                            // only return LoadDataRow count for first resultset
                            // not secondary or chaptered results
                            rowsAddedToDataSet = count;
                        }
                    }
                }
                finally
                {
                    mapping.DataTable.EndLoadData();
                }
                if (null != datatable)
                {
                    break; // do not read remaining results in single DataTable case
                }
            } while (await FillNextResultAsync(dataReader));

            return rowsAddedToDataSet;
        }

        private async Task<int> FillLoadDataRowChunkAsync(SchemaMapping mapping, int startRecord, int maxRecords)
        {
            DataReaderContainer dataReader = mapping.DataReader;

            while (0 < startRecord)
            {
                if (!await dataReader.ReadAsync())
                {
                    // there are no more rows on first resultset
                    return 0;
                }
                --startRecord;
            }

            int rowsAddedToDataSet = 0;
            if (0 < maxRecords)
            {
                while ((rowsAddedToDataSet < maxRecords) && await dataReader.ReadAsync())
                {
                    if (_hasFillErrorHandler)
                    {
                        try
                        {
                            await mapping.LoadDataRowWithClear();
                            rowsAddedToDataSet++;
                        }
                        catch (Exception e)
                        {
                            // 
                            if (!ADP.IsCatchableExceptionType(e))
                            {
                                throw;
                            }
                            ADP.TraceExceptionForCapture(e);
                            OnFillErrorHandler(e, mapping.DataTable, mapping.DataValues);
                        }
                    }
                    else
                    {
                        await mapping.LoadDataRow();
                        rowsAddedToDataSet++;
                    }
                }
                // skip remaining rows of the first resultset
            }
            else
            {
                rowsAddedToDataSet = await FillLoadDataRowAsync(mapping);
            }
            return rowsAddedToDataSet;
        }

        private async Task<int> FillLoadDataRowAsync(SchemaMapping mapping)
        {
            int rowsAddedToDataSet = 0;
            DataReaderContainer dataReader = mapping.DataReader;
            if (_hasFillErrorHandler)
            {
                while (await dataReader.ReadAsync())
                { // read remaining rows of first and subsequent resultsets
                    try
                    {
                        // only try-catch if a FillErrorEventHandler is registered so that
                        // in the default case we get the full callstack from users
                        await mapping.LoadDataRowWithClear();
                        rowsAddedToDataSet++;
                    }
                    catch (Exception e)
                    {
                        // 
                        if (!ADP.IsCatchableExceptionType(e))
                        {
                            throw;
                        }
                        ADP.TraceExceptionForCapture(e);
                        OnFillErrorHandler(e, mapping.DataTable, mapping.DataValues);
                    }
                }
            }
            else
            {
                while (await dataReader.ReadAsync())
                { // read remaining rows of first and subsequent resultset
                    await mapping.LoadDataRow();
                    rowsAddedToDataSet++;
                }
            }
            return rowsAddedToDataSet;
        }

        private SchemaMapping FillMappingInternal(DataSet dataset, DataTable datatable, string srcTable, DataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            bool withKeyInfo = (MissingSchemaAction.AddWithKey == MissingSchemaAction);
            string tmp = null;
            if (null != dataset)
            {
                tmp = DataAdapter.GetSourceTableName(srcTable, schemaCount);
            }
            return new SchemaMapping(this, dataset, datatable, dataReader, withKeyInfo, SchemaType.Mapped, tmp, true, parentChapterColumn, parentChapterValue);
        }

        private SchemaMapping FillMapping(DataSet dataset, DataTable datatable, string srcTable, DataReaderContainer dataReader, int schemaCount, DataColumn parentChapterColumn, object parentChapterValue)
        {
            SchemaMapping mapping = null;
            if (_hasFillErrorHandler)
            {
                try
                {
                    // only try-catch if a FillErrorEventHandler is registered so that
                    // in the default case we get the full callstack from users
                    mapping = FillMappingInternal(dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue);
                }
                catch (Exception e)
                {
                    // 
                    if (!ADP.IsCatchableExceptionType(e))
                    {
                        throw;
                    }
                    ADP.TraceExceptionForCapture(e);
                    OnFillErrorHandler(e, null, null);
                }
            }
            else
            {
                mapping = FillMappingInternal(dataset, datatable, srcTable, dataReader, schemaCount, parentChapterColumn, parentChapterValue);
            }
            return mapping;
        }

        private async Task<bool> FillNextResultAsync(DataReaderContainer dataReader)
        {
            bool result = true;
            if (_hasFillErrorHandler)
            {
                try
                {
                    // only try-catch if a FillErrorEventHandler is registered so that
                    // in the default case we get the full callstack from users
                    result = await dataReader.NextResultAsync();
                }
                catch (Exception e)
                {
                    // 
                    if (!ADP.IsCatchableExceptionType(e))
                    {
                        throw;
                    }
                    ADP.TraceExceptionForCapture(e);
                    OnFillErrorHandler(e, null, null);
                }
            }
            else
            {
                result = await dataReader.NextResultAsync();
            }
            return result;
        }

        [EditorBrowsableAttribute(EditorBrowsableState.Advanced)] // MDAC 69508
        virtual public IDataParameter[] GetFillParameters()
        { // V1.0.3300
            return new IDataParameter[0];
        }

        internal DataTableMapping GetTableMappingBySchemaAction(string sourceTableName, string dataSetTableName, MissingMappingAction mappingAction)
        {
            return DataTableMappingCollection.GetTableMappingBySchemaAction(_tableMappings, sourceTableName, dataSetTableName, mappingAction);
        }

        internal int IndexOfDataSetTable(string dataSetTable)
        {
            if (null != _tableMappings)
            {
                return TableMappings.IndexOfDataSetTable(dataSetTable);
            }
            return -1;
        }

        virtual protected void OnFillError(FillErrorEventArgs value)
        { // V1.2.3300, DbDataAdapter V1.0.3300
            FillErrorEventHandler handler = (FillErrorEventHandler)Events[EventFillError];
            if (null != handler)
            {
                handler(this, value);
            }
        }

        private void OnFillErrorHandler(Exception e, DataTable dataTable, object[] dataValues)
        {
            FillErrorEventArgs fillErrorEvent = new FillErrorEventArgs(dataTable, dataValues);
            fillErrorEvent.Errors = e;
            OnFillError(fillErrorEvent);

            if (!fillErrorEvent.Continue)
            {
                if (null != fillErrorEvent.Errors)
                {
                    throw fillErrorEvent.Errors;
                }
                throw e;
            }
        }

        virtual public async Task<int> UpdateAsync(DataSet dataSet)
        { // V1.0.3300
            throw ADP.NotSupported();
        }

        // used by FillSchema which returns an array of datatables added to the dataset
        static private DataTable[] AddDataTableToArray(DataTable[] tables, DataTable newTable)
        {
            for (int i = 0; i < tables.Length; ++i)
            { // search for duplicates
                if (tables[i] == newTable)
                {
                    return tables; // duplicate found
                }
            }
            DataTable[] newTables = new DataTable[tables.Length + 1]; // add unique data table
            for (int i = 0; i < tables.Length; ++i)
            {
                newTables[i] = tables[i];
            }
            newTables[tables.Length] = newTable;
            return newTables;
        }

        // dynamically generate source table names
        static private string GetSourceTableName(string srcTable, int index)
        {
            //if ((null != srcTable) && (0 <= index) && (index < srcTable.Length)) {
            if (0 == index)
            {
                return srcTable; //[index];
            }
            return srcTable + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    internal sealed class LoadAdapter : DataAdapter
    {
        internal LoadAdapter()
        {
        }

        internal async Task<int> FillFromReaderAsync(DataTable[] dataTables, IDataReader dataReader, int startRecord, int maxRecords)
        {
            return await FillAsync(dataTables, dataReader, startRecord, maxRecords);
        }
    }
}