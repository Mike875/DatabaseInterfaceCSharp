using DatabaseInterface.Enums;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DatabaseInterface.Models
{
    /// <summary>
    /// A class used to interface with the database and manipulate data within it
    /// without needing to create multiple variables each time
    /// </summary>
    public class RecordSet : IDisposable
    {
        #region PrivateFields
        /// <summary>
        /// The command object used for interacting with the database
        /// </summary>
        private SqlCommand _Command;
        
        /// <summary>
        /// The connection object used for sending commands to the database
        /// </summary>
        private SqlConnection _Connection;
        
        /// <summary>
        /// The object used to retrieve data from the database
        /// </summary>
        private SqlDataReader _DataReader;
        
        /// <summary>
        /// Whether or not the object has been destroyed
        /// </summary>
        private bool _DisposedValue;

        /// <summary>
        /// A collection of parameters to insert into the query
        /// </summary>
        private List<SqlParameter> _Parameters;
        
        /// <summary>
        /// The time it took to execute the query
        /// </summary>
        private TimeSpan _QueryTime;
        
        /// <summary>
        /// The time that the query started to execute in Ticks
        /// </summary>
        private long _StartTime;
        
        /// <summary>
        /// The current state the recordset is in
        /// </summary>
        private EState _State;
        
        /// <summary>
        /// The amount of time to wait before the query execution times out
        /// </summary>
        private int _TimeOut;
        #endregion

        #region PrivateReadOnlyFields
        private readonly string _ConnectionString;
        #endregion

        #region PublicComplexProperties
        /// <summary>
        /// Gets the name of the current database to which the client is connected
        /// </summary>
        public string Database
        {
            get
            {
                return _Connection.Database;
            }
        }

        /// <summary>
        /// Get the value that indicates the depth of nesting in the current row
        /// </summary>
        public int Depth
        {
            get
            {
                return _DataReader.Depth;
            }
        }

        /// <summary>
        /// Get the amount of time it took for a query to execute
        /// </summary>
        public TimeSpan ExecutionTime
        {
            get
            {
                if(_QueryTime == null)
                {
                    throw new Exception("Cannot determine execution time if no query has been executed");
                }else
                {
                    return _QueryTime;
                }
            }
        }

        /// <summary>
        /// Return the total number of columns in the current row
        /// </summary>
        public int FieldCount
        {
            get
            {
                return _DataReader.FieldCount;
            }
        }

        /// <summary>
        /// Get whether or not the recordset has any records
        /// </summary>
        public bool HasRows
        {
            get
            {
                return _DataReader.HasRows;
            }
        }

        /// <summary>
        /// Get whether or not the data reader is open
        /// </summary>
        public bool IsClosed
        {
            get
            {
                return _DataReader.IsClosed;
            }
        }

        /// <summary>
        /// Return whether or not there is still data available and move to the next record
        /// </summary>
        public bool Read
        {
            get
            {
                return _DataReader.Read();
            }
        }

        /// <summary>
        /// Get the number of records in the currently open recordset
        /// </summary>
        public int RecordCount
        {
            get
            {
                return GetRecordCount();
            }
        }

        /// <summary>
        /// Gets a string that contains the version of the instance of SQL Server to which the client is connected
        /// </summary>
        public string ServerVersion
        {
            get
            {
                return _Connection.ServerVersion;
            }
        }

        /// <summary>
        /// Get the state of the current recordset
        /// </summary>
        public EState State
        {
            get
            {
                return _State;
            }
        }

        /// <summary>
        /// Get the number of fields in the recordset that are not hidden
        /// </summary>
        public int VisibleFieldCount
        {
            get
            {
                return _DataReader.VisibleFieldCount;
            }
        }
        #endregion

        #region Constructors
        /// <summary>
        /// Instantiate the object and set the connection string but don't open the connection
        /// </summary>
        public RecordSet()
        {
            _ConnectionString = DatabaseManager.Instance().ConnectionString;
            _Parameters = new List<SqlParameter>();
            _TimeOut = 0;
        }

        /// <summary>
        /// Instantiate the object and set the connection string but don't open the connection
        /// </summary>
        /// <param name="connectionString">The connection string used to reference the database</param>
        public RecordSet(string connectionString)
        {
            _ConnectionString = connectionString;
            _Parameters = new List<SqlParameter>();
            _TimeOut = 0;
        }

        /// <summary>
        /// Instantiate the object and open the connection with the given sql command
        /// </summary>
        /// <param name="sqlCommand">The SQL command to execute</param>
        /// <param name="connectionString">The connection string used to reference the database</param>
        public RecordSet(string sqlCommand, string connectionString)
        {
            _ConnectionString = connectionString;
            _Parameters = new List<SqlParameter>();
            _TimeOut = 0;
            Open(sqlCommand);
        }

        /// <summary>
        /// Instantiate the object and open the connection with the given sql command
        /// </summary>
        /// <param name="sqlCommand">The SQL command to execute</param>
        /// <param name="connectionString">The connection string used to reference the database</param>
        /// <param name="timeOut">The amount of time to wait for the execution to finish</param>
        public RecordSet(string sqlCommand, string connectionString, int timeOut)
        {
            _ConnectionString = connectionString;
            _Parameters = new List<SqlParameter>();
            _TimeOut = timeOut;
            Open(sqlCommand);
        }
        #endregion

        #region PrivateMethods
        /// <summary>
        /// Add each of the parameters into the SQL command
        /// </summary>
        public void AddParameters()
        {
            foreach(SqlParameter parameter in _Parameters)
            {
                if (!_Command.Parameters.Contains(parameter)) { _Command.Parameters.Add(parameter); }
            }
        }
        
        /// <summary>
        /// Close the connection to the database
        /// </summary>
        private void CloseConnection()
        {
            if (_Connection == null) { throw new Exception("Cannot close connection that is not open"); }
            if(_Connection.State == System.Data.ConnectionState.Closed) { throw new Exception("Cannot close connection that is not open"); }

            _Connection.Close();
            _Connection = null;
        }
        
        /// <summary>
        /// Check whether or not the command object is valid within this class
        /// </summary>
        /// <returns>
        /// Whether or not the object is instantiated and the necessary properties are populated
        ///</returns>
        private bool CommandIsValid()
        {
            if(_Command == null) { return false; }
            if(_Command.CommandText == "") { return false; }

            return true;
        }
        
        /// <summary>
        /// Checks whether or not the connection object has been instantiated and populated for use
        /// </summary>
        /// <returns>
        /// Whether or not the connection object is in a usable state
        /// </returns>
        private bool ConnectionIsValid()
        {
            if(_Connection == null) { return false; }
            if(_Connection.State == System.Data.ConnectionState.Broken || _Connection.State == System.Data.ConnectionState.Closed) { return false; }

            return true;
        }
        
        /// <summary>
        /// Get the current number of records in the recordset
        /// </summary>
        /// <returns>
        /// The number of records in the current recordset
        /// </returns>
        public int GetRecordCount()
        {
            int recordCounter;
            SqlDataReader tempReader;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read data when object is not open"); }

            recordCounter = 0;
            _DataReader = null;
            tempReader = _Command.ExecuteReader();

            while (tempReader.Read())
            {
                recordCounter += 1;
            }

            tempReader = null;

            _DataReader = _Command.ExecuteReader();

            return recordCounter;
        }
        
        /// <summary>
        /// Open a connection to the database
        /// </summary>
        private void OpenConnection()
        {
            if(_Connection != null)
            {
                if(_Connection.State == System.Data.ConnectionState.Open) { throw new Exception("Cannot open connection when connection is already active"); }
            }

            _Connection = new SqlConnection(_ConnectionString);
            _Connection.Open();
        }
        
        /// <summary>
        /// Checks whether or not the date reader is in a valid/readable state
        /// </summary>
        /// <returns>
        /// Whether or not there is data present and available to read
        /// </returns>
        private bool ReaderIsValid()
        {
            if (!ConnectionIsValid()) { return false; }
            if(_DataReader == null) { return false; }
            if (_DataReader.IsClosed) { return false; }

            return true;
        }
        
        /// <summary>
        /// Set the start time for the query timer and reset the timespan variable
        /// </summary>
        private void StartTimer()
        {
            _QueryTime = new TimeSpan();
            _StartTime = DateTime.Now.Ticks;
        }
        
        /// <summary>
        /// Measure the time it took for the query to execute and create a new instance of <c>_QueryTime</c> 
        /// to display this
        /// </summary>
        private void StopTimer()
        {
            _QueryTime = new TimeSpan(DateTime.Now.Ticks - _StartTime);
            _StartTime = 0;
        }
        #endregion

        #region PublicMethods
        /// <summary>
        /// Add a sql parameter to the command
        /// </summary>
        /// <param name="parameter">The sql parameter to add</param>
        public void AddParameter(SqlParameter parameter)
        {
            if (!_Parameters.Contains(parameter)) { _Parameters.Add(parameter); }
            else { throw new Exception("Parameter already exists in recordset"); }
        }

        /// <summary>
        /// Close all connections, commands and data readers
        /// </summary>
        public void Close()
        {
            if (!ReaderIsValid()) { throw new Exception("Cannot close object that is not open"); }

            _State = EState.Closed;
            _DataReader = null;
            _Command = null;
            _Parameters = new List<SqlParameter>();

            CloseConnection();
        }

        /// <summary>
        /// Execute the given command but don't return any data
        /// </summary>
        /// <param name="sqlCommand">The SQL command to execute</param>
        public void Execute(string sqlCommand)
        {
            if (!CommandIsValid()) { throw new Exception("Command instance is not valid"); }

            Execute(sqlCommand, _TimeOut);
        }

        /// <summary>
        /// Execute the given command but don't return any data
        /// </summary>
        /// <param name="sqlCommand">The SQL command to execute</param>
        /// <param name="timeOut">The amount of time to wait before the query execution times out</param>
        public void Execute(string sqlCommand, int timeOut)
        {
            if (!CommandIsValid()) { throw new Exception("Command instance is not valid"); }

            _State = EState.Executing;

            try { OpenConnection(); }
            catch { throw; }

            _Command = new SqlCommand(sqlCommand, _Connection);
            _Command.CommandTimeout = timeOut;
            AddParameters();
            StartTimer();
            _Command.ExecuteNonQuery();
            StopTimer();

            _Command = null;
            CloseConnection();
            _State = EState.Closed;
        }

        /// <summary>
        /// Execute multiple commands with one reference
        /// </summary>
        /// <param name="sqlCommands">The list of commands to execute</param>
        public void Execute(List<string> sqlCommands)
        {
            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            foreach (string command in sqlCommands)
            {
                Execute(command);
            }
        }

        /// <summary>
        /// Execute multiple commands with one reference
        /// </summary>
        /// <param name="sqlCommands">The list of commands to execute</param>
        /// <param name="timeOut">The amount of time to wait before the query execution times out</param>
        public void Execute(List<string> sqlCommands, int timeOut)
        {
            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            foreach (string command in sqlCommands)
            {
                Execute(command, timeOut);
            }
        }
        
        /// <summary>
        /// Execute a command and return the first field in the first record of the result set
        /// </summary>
        /// <param name="command">The sql command to execute against the database</param>
        public object ExecuteScalar(string sqlCommand)
        {
            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            return ExecuteScalar(sqlCommand, _TimeOut);
        }
        
        /// <summary>
        /// Execute a command and return the first field in the first record of the result set
        /// </summary>
        /// <param name="command">The sql command to execute against the database</param>
        /// <param name="timeOut">The amount of time to wait before the query execution times out</param>
        public object ExecuteScalar(string sqlCommand, int timeOut)
        {
            object retVal;

            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            _State = EState.Executing;

            try { OpenConnection(); }
            catch { throw; }

            _Command = new SqlCommand(sqlCommand, _Connection);
            _Command.CommandTimeout = timeOut;
            AddParameters();
            StartTimer();
            retVal = _Command.ExecuteScalar();
            StopTimer();
            CloseConnection();

            _State = EState.Closed;

            return retVal;
        }

        /// <summary>
        /// Returns whether or not the given field value is null or not
        /// </summary>
        /// <param name="fieldIndex">The field to check the value of in the current record</param>
        public bool FieldIsNull(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetValue(fieldIndex) == DBNull.Value;
        }

        /// <summary>
        /// Returns whether or not the given field value is null or not
        /// </summary>
        /// <param name="fieldName">The field to check the value of in the current record</param>
        public bool FieldIsNull(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return FieldIsNull(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the number of records affected by the given command once it's been executed
        /// </summary>
        /// <param name="command">The SQL command to execute</param>
        public int GetAffectedRecordCount(string sqlCommand)
        {
            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            return GetAffectedRecordCount(sqlCommand, _TimeOut);
        }
        
        /// <summary>
        /// Return the number of records affected by the given command once it's been executed
        /// </summary>
        /// <param name="command">The SQL command to execute</param>
        /// <param name="timeOut">The amount of time to wait before the query execution times out</param>
        public int GetAffectedRecordCount(string sqlCommand, int timeOut)
        {
            int affectedRecordCount;

            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            affectedRecordCount = 0;
            _State = EState.Executing;
            _Command = new SqlCommand(sqlCommand, _Connection);
            AddParameters();
            StartTimer();
            affectedRecordCount = _Command.ExecuteNonQuery();
            StopTimer();
            CloseConnection();
            _State = EState.Open;

            return affectedRecordCount;
        }

        /// <summary>
        /// Return the total number of records affected from a collection of sql commands
        /// </summary>
        /// <param name="sqlCommands">A collection of commands to execute</param>
        public int GetAffectedRecordCount(List<string> sqlCommands)
        {
            int totalCount;

            if (!ConnectionIsValid()) { throw new Exception("Cannot execute command when connection is closed"); }

            totalCount = 0;
            _State = EState.Executing;

            foreach(string command in sqlCommands)
            {
                totalCount += GetAffectedRecordCount(command);
            }

            _State = EState.Open;

            return totalCount;
        }

        /// <summary>
        /// Return a column as a comma seperated list
        /// </summary>
        /// <param name="columnIndex">The column ordinal to return</param>
        /// <returns>A comma seperated list of fields</returns>
        public string GetColumnAsCsv(int columnIndex)
        {
            string columnAsCsv;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            columnAsCsv = "";

            while (_DataReader.Read())
            {
                columnAsCsv = string.Concat(columnAsCsv, ", ", GetFieldString(columnIndex));
            }

            return columnAsCsv.Substring(2);
        }

        /// <summary>
        /// Return a column as a comma seperated list
        /// </summary>
        /// <param name="columnName">The column name to return</param>
        /// <returns>A comma seperated list of fields</returns>
        public string GetColumnAsCsv(string columnName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetColumnAsCsv(GetFieldOrdinal(columnName));
        }
        
        /// <summary>
        /// Create a dictionary of columns in the returned data
        /// </summary>
        /// <returns></returns>
        public Dictionary<int, string> GetColumnsAsDictionary()
        {
            Dictionary<int, string> columnsAsDictionary;
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }


            if (!ConnectionIsValid()) { throw new Exception(""); }
            columnsAsDictionary = new Dictionary<int, string>();
            _State = EState.Executing;

            for(int i = 0; i < _DataReader.FieldCount - 1; i++)
            {
                columnsAsDictionary.Add(i, GetFieldName(i));
            }

            _State = EState.Open;

            return columnsAsDictionary;
        }

        /// <summary>
        /// Return the value of a given column based on the column index
        /// </summary>
        /// <param name="fieldIndex">The position of a column within the record</param>
        /// <returns>A field in a record</returns>
        public object GetField(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetValue(fieldIndex);
        }
        
        /// <summary>
        /// Return the value of the given field for the current record
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>The value of the given field for the current record</returns>
        public object GetField(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetField(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a boolean value 
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public bool GetFieldBool(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetBoolean(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a boolean
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a boolean</returns>
        public bool GetFieldBool(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldBool(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a byte
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public byte GetFieldByte(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetByte(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a byte
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a byte</returns>
        public byte GetFieldByte(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldByte(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a byte array
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <param name="dataIndex"></param>
        /// <param name="Buffer"></param>
        /// <param name="bufferIndex"></param>
        /// <param name="length"></param>
        /// <returns>The value of the column with the given index</returns>
        public long GetFieldBytes(int fieldIndex, long dataIndex, byte[] buffer, int bufferIndex, int length)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetBytes(fieldIndex, dataIndex, buffer, bufferIndex, length);
        }
        
        /// <summary>
        /// Get the given field as a Byte array
        /// </summary>
        /// <param name="fieldName">The name of the field to return</param>
        /// <param name="dataIndex"></param>
        /// <param name="Buffer"></param>
        /// <param name="bufferIndex"></param>
        /// <param name="length"></param>
        /// <returns>A byte array</returns>
        public long GetFieldBytes(string fieldName, long dataIndex, byte[] buffer, int bufferIndex, int length)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldBytes(GetFieldOrdinal(fieldName), dataIndex, buffer, bufferIndex, length);
        }
        
        /// <summary>
        /// Return the field as a DateTime
        /// Date time fields must be in the format 'yyyy-MM-dd HH:mm:ss'
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public DateTime GetFieldDateTime(int fieldIndex)
        {
            string[] allowableFormats;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            allowableFormats = new string[1];
            allowableFormats[0] = "yyyy-MM-dd HH:mm:ss";

            return GetFieldDateTime(fieldIndex, allowableFormats);
        }
        
        /// <summary>
        /// Return the field as a DateTime
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <param name="allowableDateFormats">An array of allowable date time formats</param>
        /// <returns>The value of the column with the given index</returns>
        public DateTime GetFieldDateTime(int fieldIndex, string[] allowableFormats)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return DateTime.ParseExact(_DataReader.GetString(fieldIndex), allowableFormats, null, System.Globalization.DateTimeStyles.None);
        }
        
        /// <summary>
        /// Get the given field as a DateTime
        /// Date time fields must be in the format 'yyyy-MM-dd HH:mm:ss'
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a DateTime</returns>
        public DateTime GetFieldDateTime(string fieldName)
        {
            string[] allowableFormats;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            allowableFormats = new string[1];
            allowableFormats[0] = "yyyy-MM-dd HH:mm:ss";

            return GetFieldDateTime(GetFieldOrdinal(fieldName), allowableFormats);
        }
        
        /// <summary>
        /// Get the given field as a DateTime
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <param name="allowableDateFormats">An array of allowable date time formats</param>
        public DateTime GetFieldDateTime(string fieldName, string[] allowableFormats)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldDateTime(GetFieldOrdinal(fieldName), allowableFormats);
        }
        
        /// <summary>
        /// Return the field as a double
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public double GetFieldDouble(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetDouble(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a Double
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a Double</returns>
        public double GetFieldDouble(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetDouble(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a decimal
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public float GetFieldFloat(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetFloat(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a Decimal
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a Decimal</returns>
        public float GetFieldFloat(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldFloat(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Get the given field as a Integer
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a Integer</returns>
        public int GetFieldInt(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetInt32(fieldIndex);
        }
        
        /// <summary>
        /// Return the field as an integer
        /// </summary>
        /// <param name="fieldName">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public int GetFieldInt(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetInt32(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Get the name of a given column index
        /// </summary>
        /// <param name="colIndex">The index at which the column appears</param>
        /// <returns>The column name</returns>
        public long GetFieldLong(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetInt64(fieldIndex);
        }
        
        /// <summary>
        /// Return the field as a long
        /// </summary>
        /// <param name="fieldName">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public long GetFieldLong(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetInt64(GetFieldOrdinal(fieldName));
        }

        /// <summary>
        /// Return the name of a field based on the given index
        /// </summary>
        /// <param name="fieldIndex">The column ordinal</param>
        /// <returns>The name of the field in the ordinal position</returns>
        public string GetFieldName(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetName(fieldIndex);
        }
        
        /// <summary>
        /// Return the ordinal index of a column
        /// </summary>
        /// <param name="colName">The name of the column index to return</param>
        /// <returns>The index of the column with the given name</returns>
        public int GetFieldOrdinal(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetOrdinal(fieldName);
        }
        
        /// <summary>
        /// Gets an object that is a representation of the underlying provider-specific field type
        /// </summary>
        /// <param name="fieldIndex">The column ordinal</param>
        /// <returns></returns>
        public Type GetProviderSpecificFieldType(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetProviderSpecificFieldType(fieldIndex);
        }
        
        /// <summary>
        /// Gets an object that is a representation of the underlying provider-specific field type
        /// </summary>
        /// <param name="fieldName">The column name</param>
        /// <returns></returns>
        public Type GetProviderSpecificFieldType(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetProviderSpecificFieldType(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a short
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public short GetFieldShort(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetInt16(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a Short
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a Short</returns>
        public short GetFieldShort(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldShort(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a SqlBinary type
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public SqlBinary GetFieldSqlBinary(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetSqlBinary(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a SqlBinary
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a SqlBinary</returns>
        public SqlBinary GetFieldSqlBinary(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldSqlBinary(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a SqlXml type
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public SqlXml GetFieldSqlXml(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetSqlXml(fieldIndex);
        }
        
        /// <summary>
        /// Return the field as a SqlXml type
        /// </summary>
        /// <param name="fieldName">The column name to return</param>
        /// <returns>The value of the column with the given name</returns>
        public SqlXml GetFieldSqlXml(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldSqlXml(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a IO.Stream type
        /// </summary>
        /// <param name="fieldName">The column name to return</param>
        /// <returns>The value of the column with the given name</returns>
        public Stream GetFieldStream(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetStream(fieldIndex);
        }
        
        /// <summary>
        /// Return the field as a IO.Stream type
        /// </summary>
        /// <param name="fieldName">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public Stream GetFieldStream(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldStream(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a string
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public string GetFieldString(int fieldIndex)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetString(fieldIndex);
        }
        
        /// <summary>
        /// Get the given field as a String
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a String</returns>
        public string GetFieldString(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.GetString(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Return the field as a Timespan
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <returns>The value of the column with the given index</returns>
        public TimeSpan GetFieldTimeSpan(int fieldIndex)
        {
            string[] allowableFormats;
            DateTime date;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            allowableFormats = new string[4];
            allowableFormats[0] = "dd-MM-yyyy HH:mm:ss";
            allowableFormats[1] = "yyyy-MM-dd HH:mm:ss";
            allowableFormats[2] = "dd-MM-yyyy";
            allowableFormats[3] = "yyyy-MM-dd";

            date = GetFieldDateTime(fieldIndex, allowableFormats);

            return new TimeSpan(date.Day, date.Hour, date.Minute, date.Second);
        }
        
        /// <summary>
        /// Return the field as a TimeSpan
        /// </summary>
        /// <param name="fieldIndex">The column index to return</param>
        /// <param name="allowableFormats">An array of allowed date time formats</param>
        /// <returns>The value of the column with the given index</returns>
        public TimeSpan GetFieldTimeSpan(int fieldIndex, string[] allowableFormats)
        {
            DateTime date;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            date = GetFieldDateTime(fieldIndex, allowableFormats);

            return new TimeSpan(date.Day, date.Hour, date.Minute, date.Second);
        }
        
        /// <summary>
        /// Get the given field as a TimeSpan
        /// Date time fields must be in the format 'yyyy-MM-dd HH:mm:ss'
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <returns>A field as a TimeSpan</returns>
        public TimeSpan GetFieldTimeSpan(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldTimeSpan(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Get the given field as a TimeSpan
        /// </summary>
        /// <param name="fieldName">The name of the field to return the value from</param>
        /// <param name="allowableFormats">An array of allowed date time formats</param>
        /// <returns>A field as a TimeSpan</returns>
        public TimeSpan GetFieldTimeSpan(string fieldName, string[] allowableFormats)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetFieldTimeSpan(GetFieldOrdinal(fieldName), allowableFormats);
        }
        
        /// <summary>
        /// Get the entire record and return it as a collection
        /// </summary>
        /// <returns>The current record as a <c>List(Of String)</c> collection</returns>
        public List<string> GetListOfFields(int fieldIndex)
        {
            List<string> retList;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            retList = new List<string>();
            _State = EState.Executing;

            while (_DataReader.Read())
            {
                retList.Add(_DataReader.GetString(fieldIndex));
            }

            _State = EState.Open;

            return retList;
        }
        
        /// <summary>
        /// Generate a collection generated from the recordset
        /// </summary>
        /// <param name="fieldName">The name of the field you want to generate a collection of</param>
        /// <returns>A collection of strings generated from the recordset</returns>
        public List<string> GetListOfFields(string fieldName)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return GetListOfFields(GetFieldOrdinal(fieldName));
        }
        
        /// <summary>
        /// Get the entire record and return it as a collection
        /// </summary>
        /// <returns>The current record as a <c>List(Of String)</c> collection</returns>
        public List<string> GetRecord()
        {
            List<string> retList;

            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }
            retList = new List<string>();
            _State = EState.Executing;

            for(int i = 0; i < _DataReader.FieldCount - 1; i++)
            {
                retList.Add(_DataReader.GetString(i));
            }

            _State = EState.Open;

            return retList;
        }
        
        /// <summary>
        /// Creates a prepared version of the command on an instance of SQL Server
        /// </summary>
        public void Prepare()
        {
            _Command.Prepare();
        }
        
        /// <summary>
        /// Open a connection to the database and populate the data reader
        /// </summary>
        /// <param name="sqlCommand">The SQL command to execute on the server</param>
        public void Open(string sqlCommand)
        {
            Open(sqlCommand, _TimeOut);
        }
        
        /// <summary>
        /// Open a connection to the database and populate the data reader
        /// </summary>
        /// <param name="sqlCommand">The SQL command to execute on the server</param>
        /// <param name="timeOut">The amount of time to wait before the query execution times out</param>
        public void Open(string sqlCommand, int timeOut)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }
            
            try { OpenConnection(); }
            catch { throw new Exception(""); }

            _Command = new SqlCommand(sqlCommand, _Connection);
            _Command.CommandTimeout = timeOut;
            AddParameters();
            StartTimer();
            _DataReader = _Command.ExecuteReader();
            StopTimer();

            _State = EState.Open;
        }
        
        /// <summary>
        /// Read the data asynchronously
        /// </summary>
        /// <returns></returns>
        public Task<bool> ReadAsync()
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.ReadAsync();
        }
        
        /// <summary>
        /// Read the data asynchronously
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction</param>
        /// <returns></returns>
        public Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (!ReaderIsValid()) { throw new Exception("Attempt to read when object is not open"); }

            return _DataReader.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// Remove a parameter from the collection of sql parameters
        /// </summary>
        /// <param name="parameter"></param>
        public void RemoveParameter(SqlParameter parameter)
        {
            if (_Parameters.Contains(parameter)) { _Parameters.Remove(parameter); }
        }
        #endregion

        #region IDisposable Support
        /// <summary>
        /// Perform any necessary actions before disposing of the object
        /// </summary>
        /// <param name="disposing">Whether or not we're disposing of the class</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_DisposedValue)
            {
                if (disposing) { Close(); }
                _DisposedValue = true;
            }
        }
        
        /// <summary>
        /// Dispose of the object
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
