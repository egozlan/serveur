using System;
using System.IO;

namespace OokcityServer
{
    public class ServerConfiguration
    {

        public ServerConfiguration()
        {

        }

        public void Load()
        {

            bool confFileExist = false;
            string configFile = "config.ini";

            do
            {
                confFileExist = System.IO.File.Exists(@configFile);
                if (!confFileExist)
                {
                    //Program.MainForm.WriteTextOnConsole("Création du Fichier de configuration (" + configFile + ").");

                    string fileName = @"config.ini";

                    //System.IO.Directory.CreateDirectory(folderName);
                    //string fileName = "_" + this.serialPort.PortName + ".txt";

                    //string pathString = System.IO.Path.Combine(folderName, fileName);
                    //pathString = @"ModemCPT/COM4.txt";

                    using (StreamWriter writer = new StreamWriter(fileName))
                    {
                        writer.WriteLine("#Port sur lequel le serveur ecoute");
                        writer.WriteLine("port=555");
                    }

                }

            }
            while (!confFileExist);

            string[] Conf = System.IO.File.ReadAllLines(@configFile);

            foreach (var configItem in Conf)
            {
                if (configItem != "")
                {
                    if (configItem.Substring(0, 1) != "#")
                    {
                        if (configItem.Contains("port"))
                        {
                            string confItemValue = configItem.Trim();
                            confItemValue = confItemValue.Replace("port=", "");
                            Program.TcpServer.SetListenPort(int.Parse(confItemValue));
                        }/*
                        else if (configItem.Contains("cadence"))
                        {
                            string confItemValue = configItem.Trim();
                            confItemValue = confItemValue.Replace("cadence=", "");
                            Program.cadence = int.Parse(confItemValue);
                        }*/
                    }
                    else
                    {
                        continue;
                    }
                }
                else
                {

                }

            }

        }


    }
}