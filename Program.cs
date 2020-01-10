using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;



namespace OokcityServer
{
    static class Program
    {
        public static TCPServer TcpServer;
        public static Form1 MainForm;


        /// <summary>
        /// Point d'entrée principal de l'application.
        /// Lancement du server TCP
        /// </summary>
        [STAThread]
        static void Main()
        {
            TcpServer = new TCPServer();
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            MainForm = new Form1();
            //Application.Run(new Form1());
            Application.Run(MainForm);

        }

    }

     

}
