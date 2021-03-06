﻿using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace OokcityServer
{
    static class MySQL
    {
        static private MySqlConnection connection;
        
        static private string server = "163.172.26.178";
        static private string database = "ookcity";
        static private string uid = "root";
        static private string password = "egozlan021109";
        
        /*
        static private string server = "localhost";
        static private string database = "ookcity";
        static private string uid = "root";
        static private string password = "Egozlan021109$";
        */

        //open connection to database
        static private bool OpenConnection()
        {
            string connectionString;
            //connectionString = "SERVER=" + server + ";" + "DATABASE=" +
            //database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";SslMode=none;";

            connectionString = "SERVER=" + server + ";" + "DATABASE=" +
            database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";

            connection = new MySqlConnection(connectionString);

            try
            {
                connection.Open();
                return true;
            }
            catch (MySqlException ex)
            {
                //When handling errors, you can your application's response based 
                //on the error number.
                //The two most common error numbers when connecting are as follows:
                //0: Cannot connect to server.
                //1045: Invalid user name and/or password.
                switch (ex.Number)
                {
                    /*
                    case 0:
                        MessageBox.Show("Cannot connect to server.  Contact administrator");
                        break;

                    case 1045:
                        MessageBox.Show("Invalid username/password, please try again");
                        break;
                        */
                }
                return false;
            }
        }

        //Close connection
        static private bool CloseConnection()
        {
            try
            {
                connection.Close();
                return true;
            }
            catch (MySqlException ex)
            {
                MessageBox.Show(ex.Message);
                return false;
            }
        }

        static public string Insert(string query)
        {
            var inseredId = string.Empty;
            //string query = "INSERT INTO tableinfo (name, age) VALUES('John Smith', '33')";

            //open connection
            if (OpenConnection() == true)
            {
                //create command and assign the query and connection from the constructor
                MySqlCommand cmd = new MySqlCommand(query, connection);

                //Execute command
                cmd.ExecuteNonQuery();
                var id = cmd.LastInsertedId;
                //close connection
                CloseConnection();

                inseredId = id.ToString();
            }

            return inseredId;
        }

        static public bool ExecuteNonQuery(string query)
        {
            bool result = false;
            //string query = "INSERT INTO tableinfo (name, age) VALUES('John Smith', '33')";

            //open connection
            if (OpenConnection() == true)
            {
                //create command and assign the query and connection from the constructor
                MySqlCommand cmd = new MySqlCommand(query, connection);

                //Execute command
                cmd.ExecuteNonQuery();

                //close connection
                CloseConnection();
                result = true;
            }

            return result;
        }

        //Select statement
        static public List<Dictionary<string, string>> Select(string query)
        {
            //string query = "SELECT * FROM partners";

            //List<Dictionary<string, string>> dataSelect = new List<Dictionary<string, string>>();
            List<Dictionary<string, string>> dataSelect = new List<Dictionary<string, string>>();

            //Open connection
            if (OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(query, connection);
                //Create a data reader and Execute the command

                try
                {
                    MySqlDataReader dataReader = cmd.ExecuteReader();
                
                    var nbField = dataReader.FieldCount;

                    //Read the data and store them in the list
                    while (dataReader.Read())
                    {
                        //var tmpList = new List<string>();

                        var dict = new Dictionary<string, string>();

                        for (int i = 0; i < nbField; i++)
                        {
                            try
                            {
                                dict.Add(dataReader.GetName(i), dataReader[i].ToString());
                            }
                            catch { }
                            
                        }

                        dataSelect.Add(dict);
                    }

                    //close Data Reader
                    dataReader.Close();

                    //close Connection
                    CloseConnection();

                    //return list to be displayed
                    return dataSelect;
                }
                catch
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

    }



}
