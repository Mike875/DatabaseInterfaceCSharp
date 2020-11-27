using DatabaseInterface.Enums;
using System;
using System.Collections.Generic;

namespace DatabaseInterface.Models
{
    /// <summary>
    /// Use this class to store references to the database and connection strings.
    /// It can then be used by classes such as <c>RecordSet</c> to connect to any
    /// database instance
    /// </summary>
    public sealed class DatabaseManager
    {
        #region PrivateFields
        /// <summary>
        /// Used for the singleton pattern. Stores an instance of this class to be instantiated once at runtime and used by the entire application.
        /// To reference this field, use <example>var = DatabaseManager.Instance()</example>
        /// </summary>
        private static DatabaseManager _Instance;
        #endregion
        
        #region PublicReadOnlyFields
        /// <summary>
        /// The connection string to use
        /// </summary>
        public readonly string ConnectionString;
        
        /// <summary>
        /// A dictionary collection of connection strings and a key pointing to each of them
        /// </summary>
        /// <returns>
        /// A database connection string
        /// </returns>
        private readonly Dictionary<string, string> ConnectionStrings;
        #endregion
        
        #region StaticMethods
        /// <summary>
        /// Gets whether or not the database manager instance has been instantiated
        /// </summary>
        /// <returns>True if an instance of _DatabaseManager exists</returns>
        public static bool IsAvailable()
        {
            return (_Instance != null);
        }
        #endregion

        #region PrivateConstructors
        /// <summary>
        /// Generate a new instance of this class and generate a dictionary of database connection strings
        /// Set the default database instance
        /// </summary>
        /// <param name="connString">The connection string the client wants to use</param>
        private DatabaseManager(EConnectionString connString)
        {
            ConnectionStrings = GetDatabaseDictionary();
            ConnectionString = GetConnectionString(connString);
        }
        
        /// <summary>
        /// Generate a new instance of this class and generate a dictionary of database connection strings
        /// Set the default database instance
        /// </summary>
        /// <param name="connString">The connection string the client wants to use</param>
        private DatabaseManager(string connString)
        {
            ConnectionStrings = GetDatabaseDictionary();
            ConnectionString = connString;
        }
        #endregion

        #region PublicConstructors
        /// <summary>
        /// Used to create a singleton instance of this class
        /// </summary>
        /// <returns>
        /// If the class has already been instantiated, return that instance of the class
        /// Otherwise, instantiate a new instance and return that
        /// </returns>
        public static DatabaseManager Instance()
        {
            if (_Instance == null) { throw new Exception(""); }

            return _Instance;
        }
        
        /// <summary>
        /// Used to create a singleton instance of this class
        /// </summary>
        /// <param name="connectionString">The default database instance</param>
        /// <returns>
        /// If the class has already been instantiated, return that instance of the class
        /// Otherwise, instantiate a new instance and return that
        /// </returns>
        public static DatabaseManager Instance(EConnectionString connString)
        {
            if(_Instance == null) { _Instance = new DatabaseManager(connString); }

            return _Instance;
        }
        
        /// <summary>
        /// Used to create a singleton instance of this class
        /// </summary>
        /// <param name="connectionString">The default database instance</param>
        /// <returns>
        /// If the class has already been instantiated, return that instance of the class
        /// Otherwise, instantiate a new instance and return that
        /// </returns>
        public static DatabaseManager Instance(string connString)
        {
            if(_Instance == null) { _Instance = new DatabaseManager(connString); }

            return _Instance;
        }
        #endregion

        #region PrivateMethods
        /// <summary>
        /// Build a dictionary collection of database instances with a key 
        /// </summary>
        /// <returns>A dictionary collection of database</returns>
        private Dictionary<string, string> GetDatabaseDictionary()
        {
            string placeHolder;
            Dictionary<string, string> databaseDictionary;
            string template;

            databaseDictionary = new Dictionary<string, string>();
            template = "Data Source=$U$;Initial Catalog=Advantage;User ID=;Password=;MultipleActiveResultSets=False";
            placeHolder = "$U$";

            databaseDictionary.Add("", template.Replace(placeHolder, ""));

            return databaseDictionary;
        }
        #endregion

        #region PublicMethods
        /// <summary>
        /// Return a collection of every available connection's key
        /// </summary>
        /// <returns>List of every key in the <c>ConnectionStrings</c> dictionary</returns>
        public List<string> GetAllInstances()
        {
            List<string> instances;

            instances = new List<string>();

            foreach(string key in ConnectionStrings.Keys)
            {
                instances.Add(key);
            }

            return instances;
        }
        
        /// <summary>
        /// Return a list of all available databases for the Active connection
        /// </summary>
        /// <returns>List of available databases</returns>
        public List<string> GetAvailableDatabases()
        {
            return GetAvailableDatabases(ConnectionString);
        }
        
        /// <summary>
        /// Return a list of all available databases for a given connection string
        /// </summary>
        /// <param name="connectionString">An enum representation of a connection string</param>
        /// <returns>List of available databases</returns>
        public List<string> GetAvailableDatabases(string connectionString)
        {
            List<string> databases;
            RecordSet rs;

            databases = new List<string>();
            rs = new RecordSet("SELECT name FROM master.sys.databases ORDER BY name ASC");

            while (rs.Read)
            {
                databases.Add(rs.GetFieldString("name"));
            }

            rs.Close();
            rs = null;

            return databases;
        }
        
        /// <summary>
        /// Generate and return a list of all available databases for a given connection string
        /// </summary>
        /// <param name="connectionString">A database connection string</param>
        /// <returns>List of available databases</returns>
        public List<string> GetAvailableDatabases(EConnectionString connectionString)
        {
            return GetAvailableDatabases(GetConnectionString(connectionString));
        }

        /// <summary>
        /// Return the connection string represented by <paramref name="connString"/> reference
        /// </summary>
        /// <param name="connString">The enum representation of the connection string you want returned from <c>ConnectionStrings</c></param>
        /// <returns>A database connection string from the <c>ConnectionStrings</c> dictionary</returns>
        /// <exception cref="Exception">Invalid connection string requested</exception>
        public string GetConnectionString(EConnectionString connectionString)
        {
            string returnConnectionString;

            if (!ConnectionStrings.TryGetValue(connectionString.ToString(), out returnConnectionString)) { throw new Exception("Invalid connection string request. Could not find connection string in collection"); }

            return returnConnectionString;
        }
        #endregion
    }
}
