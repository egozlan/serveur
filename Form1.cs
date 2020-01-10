using System;
using System.Drawing;
using System.Windows.Forms;
using Sockets.Plugin;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;

using System.Threading;
using System.Threading.Tasks;

namespace OokcityServer
{
    public partial class Form1 : Form
    {
        delegate void WriteTextOnConsoleDelegate(string text);
        delegate void ChangeStateOfStartButtonDelegate(int state);
        delegate void AddClientRowDelegate(string id,string addr,string state);
        delegate void RemoveClientRowDelegate(string id);
        delegate void AddPartnerRowDelegate(string id, string addr, string state);
        delegate void RemovePartnerRowDelegate(string id);
        delegate void ResetScreenDelegate(); 

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            //var t = MySQL.Select();
            //var deb = t[0]["Id"];
            //Demarage automatique
            var conf = new ServerConfiguration();
            conf.Load();
            Program.TcpServer.Start();

            /*
            ServerStartButton.BackColor = Color.Red;
            ServerStartButton.ForeColor = Color.White;
            ServerStartButton.Text = "STOPPED";            
            */
        }

        /// <summary>
        /// Event on the start server button
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void ServerStartButton_Click(object sender, EventArgs e)
        {
            ResetScreen();

            Program.TcpServer.StripeTest();

            //Program.TcpServer.SendTestMsg();


            //TODO : Suprrimer ligne de code de test

            //var test = Task.Run(()=> { Program.TcpServer.testMsg(); });
            //Program.TcpServer.testMsg();

            /*
            if (Program.TcpServer.IsServerStarted()==false)
            {
               Program.TcpServer.Start();
            }
            else
            {
               Program.TcpServer.Stop();
            }
            */
        }

        /// <summary>
        /// Function to write text on the ConsoleTextBox
        /// </summary>
        /// <param name="text"></param>
        public void WriteTextOnConsole(string text)
        {
            if (this.ConsoleTextBox.InvokeRequired)
            {
                WriteTextOnConsoleDelegate d = new WriteTextOnConsoleDelegate(WriteTextOnConsole);
                this.Invoke(d, new object[] { text });
            }
            else
            {
                this.ConsoleTextBox.AppendText("\r\n"+text);
                // Scrool automatique de la textBox.
                ConsoleTextBox.SelectionStart = ConsoleTextBox.Text.Length;
                ConsoleTextBox.ScrollToCaret();
            }
        }
        
        /// <summary>
        /// Change the state of the start button (color and text)
        /// </summary>
        /// <param name="state"></param>
        public void ChangeStateOfStartButton(int state=0)
        {
            // InvokeRequired required compares the thread ID of the  
            // calling thread to the thread ID of the creating thread.  
            // If these threads are different, it returns true.  
            if (this.ServerStartButton.InvokeRequired)
            {
                ChangeStateOfStartButtonDelegate d = new ChangeStateOfStartButtonDelegate(ChangeStateOfStartButton);
                this.Invoke(d, new object[] { state });
            }
            else
            {
                switch (state)
                {
                    case 0:
                        ServerStartButton.BackColor = Color.Red;
                        ServerStartButton.ForeColor = Color.White;
                        ServerStartButton.Text = "STOPPED";
                        break;
                    case 1:
                        this.ServerStartButton.BackColor = Color.GreenYellow;
                        this.ServerStartButton.ForeColor = Color.Black;
                        this.ServerStartButton.Text = "STARTED";
                        break;
                    
                }

            }

        }

        /// <summary>
        /// Add a row on the client listView
        /// </summary>
        /// <param name="id"></param>
        /// <param name="addr"></param>
        /// <param name="state"></param>
        public void AddClientRow(string id,string addr,string state)
        {
            if (this.listView1.InvokeRequired)
            {  
                AddClientRowDelegate d = new AddClientRowDelegate(AddClientRow);
                this.Invoke(d, new object[] { id,addr,state });
            }
            else
            {
                // Supprime le la ligne si elle est deja presente
                string[] row = { id };
                var listViewItem = this.listView1.Items;
                var itemToRemove = new ListViewItem();
                for (int i = 0; i < listViewItem.Count; i++)
                {
                    if (listViewItem[i].SubItems[0].Text == id)
                    {
                        itemToRemove = listViewItem[i];
                    }
                }

                listView1.Items.Remove(itemToRemove);

                // ajoute la ligne avec les nouvelle information(exemple : mise a jours de l'adresse ip)
                string[] newRow = { id,addr,state };
                var newListViewItem = new ListViewItem(newRow);
                listView1.Items.Add(newListViewItem);
            }
        }

        /// <summary>
        /// Remove a row from the client list view
        /// </summary>
        /// <param name="id"></param>
        public void RemoveClientRow(string id)
        {
            if (this.listView1.InvokeRequired)
            {
                RemoveClientRowDelegate d = new RemoveClientRowDelegate(RemoveClientRow);
                this.Invoke(d, new object[] { id });
            }
            else
            {   
                string[] row = { id };
                var listViewItem = this.listView1.Items;
                var itemToRemove = new ListViewItem();
                for (int i=0; i< listViewItem.Count; i++)
                {
                    if (listViewItem[i].SubItems[0].Text == id)
                    {
                        itemToRemove = listViewItem[i];
                    }
                }

                listView1.Items.Remove(itemToRemove);
                
            }
        }

        /// <summary>
        /// Add a row on the Partner List View
        /// </summary>
        /// <param name="id"></param>
        /// <param name="addr"></param>
        /// <param name="state"></param>
        public void AddPartnerRow(string id, string addr, string state)
        {
            if (this.listView2.InvokeRequired)
            {
                AddPartnerRowDelegate d = new AddPartnerRowDelegate(AddPartnerRow);
                this.Invoke(d, new object[] { id, addr, state });
            }
            else
            {
                // Supprime le la ligne si elle est deja presente
                string[] row = { id };
                var listViewItem = this.listView2.Items;
                var itemToRemove = new ListViewItem();
                for (int i = 0; i < listViewItem.Count; i++)
                {
                    if (listViewItem[i].SubItems[0].Text == id)
                    {
                        itemToRemove = listViewItem[i];
                    }
                }

                listView2.Items.Remove(itemToRemove);

                // ajoute la ligne avec les nouvelle information(exemple : mise a jours de l'adresse ip)
                string[] newRow = { id, addr, state };
                var newListViewItem = new ListViewItem(newRow);
                listView2.Items.Add(newListViewItem);
            }
        }

        /// <summary>
        /// Remove a row from the partner ListVew
        /// </summary>
        /// <param name="id"></param>
        public void RemovePartnerRow(string id)
        {
            if (this.listView2.InvokeRequired)
            {
                RemovePartnerRowDelegate d = new RemovePartnerRowDelegate(RemovePartnerRow);
                this.Invoke(d, new object[] { id });
            }
            else
            {
                string[] row = { id };
                var listViewItem = this.listView2.Items;
                var itemToRemove = new ListViewItem();
                for (int i = 0; i < listViewItem.Count; i++)
                {
                    if (listViewItem[i].SubItems[0].Text == id)
                    {
                        itemToRemove = listViewItem[i];
                    }
                }

                listView2.Items.Remove(itemToRemove);
            }
        }

        /// <summary>
        /// CLS de la console
        /// </summary>
        public void ResetScreen()
        {
            if (this.ConsoleTextBox.InvokeRequired)
            {
                ResetScreenDelegate d = new ResetScreenDelegate(ResetScreen);
            }
            else
            {
                ConsoleTextBox.Text = string.Empty;
            }
        }

    }
}
