using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Sockets.Plugin;
using Sockets.Plugin.Abstractions;
using Stripe;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace OokcityServer
{

    class TCPServer
    {
        private int ListenPort = 24170;
        private TcpSocketListener listener;
        private bool serverIsStarted;
        private bool cleanUpTable;
        private readonly Dictionary<string, ITcpSocketClient> SocketPool;
        private readonly Dictionary<string, string> TokenPool;


        /* Developpement APN Certificats */
        private string ApnCerFilepath = "partner_apn_dev.p12";
        private string ApnCerPwd = "hedbox";
        private string ApnCerFinalFilepath = "ookcity_apn_dev.p12";
        private string ApnCerFinalPwd = "hedbox";
        /* Production APN certificats */
        //private string ApnCerFilepath = "ookcitypartner.p12";
        //private string ApnCerPwd = "hedbox";        
        //private string ApnCerFinalFilepath = "ookcity.p12";
        //private string ApnCerFinalPwd = "hedbox";

        public bool IdentificationBool { get; set; }


        private string Version = "0.1.2";


        public TCPServer()
        {
            //this.ListenPort = port;
            listener = new TcpSocketListener();
            serverIsStarted = false;
            cleanUpTable = false;
            SocketPool = new Dictionary<string, ITcpSocketClient>();
            TokenPool = new Dictionary<string, string>();

            IdentificationBool = true;

            this.Init();

        }

        public void StripeTest()
        {
            StripeConfiguration.ApiKey = "sk_test_dBR2X5yRuQklppKxRz7jCbuT";

            var service = new PaymentIntentService();
            var paymentIntent = service.Get("pi_1FICiQJq5arTgj4oB9FgFwrI");
            var trGroupe = paymentIntent.TransferGroup;

            var trService = new TransferService();
            var trOptions = new TransferListOptions
            {
                TransferGroup = trGroupe
            };
            var transfers = trService.List(trOptions);

            StripeConfiguration.ApiKey = "sk_test_dBR2X5yRuQklppKxRz7jCbuT";

            var requestOption = new RequestOptions();
            //requestOption.StripeConnectAccountId = "acct_1ExYxyJSf5Bp2SSh";
            requestOption.StripeAccount = "acct_1ExYxyJSf5Bp2SSh";

            var poService = new PayoutService();
            var options = new PayoutListOptions { Limit = 3 };
            var payouts = poService.List(options, requestOption);

            var payoutNeeded = payouts.ElementAt(0).Id;

            var bTransactionService = new BalanceTransactionService();
            StripeList<BalanceTransaction> transactions = bTransactionService.List(
              new BalanceTransactionListOptions
              {
                  Limit = 100,
                 // PayoutId = payoutNeeded,
                  Payout = payoutNeeded
              }, requestOption);


            var balanceTransactionId = payouts.ElementAt(0).BalanceTransactionId;

            var balanceTransactionServices = new BalanceTransactionService();

           // var balanceTransactionService = balanceTransactionServices.Get(balanceTransactionId, requestOption);

            var tsUnix = payouts.ElementAt(0).Created;

            var transferService = new TransferService();
            var transferOptions = new TransferListOptions
            {
                Limit = 3,
                Created = tsUnix
            };
            var listTransfers = transferService.List(transferOptions, requestOption);

            var eof = "EOF";
        }

        public bool SetListenPort(int port)
        {
            this.ListenPort = port;
            return true;
        }

        /// <summary>
        /// Initialisation of the Tcp Server
        /// </summary>
        private void Init()
        {
            MySqlRequest mySqlRequest = new MySqlRequest();
            // Initialisation des socket dans la database
            var sql_init_sockets_part = "UPDATE partners SET socket_id = NULL,online = 0,available=0";
            if (mySqlRequest.ExecuteNonQuery(sql_init_sockets_part) == false)
            {
                //Program.MainForm.WriteTextOnConsole("Init partners socket mySqlRequest error");
                Console.WriteLine("Init partners socket mySqlRequest error");
            }
            var sql_init_sockets_cli = "UPDATE client SET socket_id = NULL";
            if (mySqlRequest.ExecuteNonQuery(sql_init_sockets_cli) == false)
            {
                //Program.MainForm.WriteTextOnConsole("Init client socket mySqlRequest error");
                Console.WriteLine("Init client socket mySqlRequest error");
            }

            GetTarifs();

            listener.ConnectionReceived += Listener_ConnectionReceived;

            // 02-03-2019
            // Thread supprimer car je ne trouve pas pourquoi a qoui ca sert.

            //Task t = new Task(CheckTcpSocket);
            //t.Start();



            /*
            // when we get connections, read byte-by-byte from the socket's read stream
            listener.ConnectionReceived += async (senderListener, args) =>            
            {
                var client = args.SocketClient;
                var x = await client.GetConnectedInterfaceAsync();
                var bytesRead = -1;
                var buf = new byte[1024];
                Program.MainForm.AddClientRow("01",client.RemoteAddress,x.ConnectionStatus.ToString());
                while (bytesRead != 0)
                {
                    bytesRead = await args.SocketClient.ReadStream.ReadAsync(buf, 0, buf.Length);
                    if (bytesRead > 0)
                    {
                        Console.WriteLine(ASCIIEncoding.ASCII.GetString(buf));
                        string value = ASCIIEncoding.ASCII.GetString(buf);
                        Array.Clear(buf, 0, buf.Length);
                        Program.MainForm.WriteTextOnConsole(value);
                        var jsonVal = JObject.Parse(value);
                        //JsonValue jsonValue_with_action = JsonValue.Parse(value.Trim());
                        //jsonValue = jsonValue_with_action["data"];
                        IList<string> keys = jsonVal.Properties().Select(p => p.Name).ToList();
                        if (keys.Contains("action"))
                        {
                        }
                    }
                }
            };
            */



        }

        /// <summary>
        /// opto
        /// </summary>
        private async void CheckTcpSocket()
        {
            while (true)
            {
                foreach (ITcpSocketClient s in SocketPool.Values.ToList())
                {
                    var data = new { action = "check", value = "ok" };

                    JObject dataToSend = JObject.FromObject(data);
                    //dataToSend["action"] = "connection_result";
                    //dataToSend["status"] = 3;

                    var serialized = JsonConvert.SerializeObject(dataToSend);
                    string input = serialized;

                    byte[] buffer = ASCIIEncoding.UTF8.GetBytes(input);

                    ClientWriteSmartResult clientWriteSmartResult = new ClientWriteSmartResult();

                    if (s.GetStream().Position == 0)
                    {
                        try
                        {

                            s.WriteStream.Write(buffer, 0, buffer.Count());
                            await s.WriteStream.FlushAsync();

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                            clientWriteSmartResult.Result = false;
                            clientWriteSmartResult.ExceptionDetails = e;

                            Program.MainForm.WriteTextOnConsole("TCP SOCKET ERROR : " + e.Message);
                            await s.DisconnectAsync();
                            var socketId = SocketPool.FirstOrDefault(x => x.Value == s).Key;
                            if (socketId != null)
                            {
                                var clientId = socketId.Remove(0, 1);
                                SocketPool.Remove(socketId);
                                Program.MainForm.RemoveClientRow(clientId);
                            }
                        }


                        if (clientWriteSmartResult.ExceptionDetails == null)
                        {
                            clientWriteSmartResult.Result = true;
                        }
                    }
                }
                Thread.Sleep(200);
            }
        }

        /// <summary>
        /// Send a remote notification to a device
        /// </summary>
        /// <param name="idSoket">Id du socket</param>
        /// <param name="notificationMessage">Message</param>
        /// <param name="notificationSound">sound of notification(optional)</param>
        public void SendClientRemoteNotification(string idSoket, string notificationMessage, string notificationSound = "default")
        {
            var tokenValue = TokenPool.FirstOrDefault(x => x.Key == idSoket).Value;
            if (tokenValue == null)
            {
                var numClient = idSoket.Replace("C", "");
                var sqlRequest = new MySqlRequest();
                var sql = "SELECT apn_token FROM client WHERE Id_client = '" + numClient + "'";
                var result = sqlRequest.Select(sql);

                tokenValue = result[0]["apn_token"];
            }

            var message = notificationMessage;
            string filepath = ApnCerFinalFilepath;
            string pwd = ApnCerFinalPwd;
            PushNotification pushNotification = new PushNotification(PushNotificationType.Development, filepath, pwd);
            PushNotificationPayload payload = new PushNotificationPayload();
            payload.deviceToken = tokenValue;
            payload.badge = 0;
            payload.sound = notificationSound;
            payload.message = message;
            pushNotification.Push(payload);
        }

        /// <summary>
        /// Fonction qui permet de nettoyer la table selection
        /// Elle géré aussi les message de time out envoyer au partenaire.
        /// </summary>
        private async void CleanUpSelectionTable()
        {
            while (cleanUpTable == true)
            {
                // Séléction de tous les enregistrement qui se trouve dans la table selection.
                var sqlRequest = new MySqlRequest();
                var sql = "SELECT * FROM selection ORDER BY time_stamp DESC";
                var result = sqlRequest.Select(sql);

                // Si on a pas de partenaire dans la table selection en attend 50 ms.
                if(result.Count == 0){Thread.Sleep(50);}
                else
                {
                    // Sinon on parcours les enregistrement et on suprrimé tous ceux supperieure a 30sec
                    // Si on a pas d'enregistrement superrieure a 30sec mais entre les deux on Sleep le tread 
                    // de 30s moins le le timestamp de l 'enregistrement le plus vieux.
                    if (result.Count > 0)
                    {
                        var n = 0;
                        var t = 0;
                        foreach (Dictionary<string, string> value in result)
                        {
                            n++;
                            var current_date = DateTime.Now;
                            //var timestamp = DateTime.Parse(value["time_stamp"].ToString()).ToString("yyyy-MM-dd HH:mm:ss");
                            var timestamp = DateTime.Parse(value["time_stamp"].ToString());
                            var diff = current_date - timestamp;
                            if (n == 1)
                            {
                                t = 30 - (int)diff.TotalSeconds;
                            }
                        }

                        var now = DateTime.Now;
                        var need_to_be_delete = now.AddSeconds(-30);
                        var needToBeDeleteStr = need_to_be_delete.ToString("yyyy-MM-dd HH:mm:ss");

                        // Select Id des partners en fin de chrono pour leur envoyer le message Time-out
                        var sqlSelectPartenrsId = "SELECT order_id,socket_id,client_id,socket_client_id FROM selection WHERE time_stamp <= '" + needToBeDeleteStr + "'";
                        Console.WriteLine(sqlSelectPartenrsId.ToString());
                        var resultSelectPartnersId = sqlRequest.Select(sqlSelectPartenrsId);
                        // Variable tableau d'Ids commande pour pouvoir envoyer un message au client finale sur le socket
                        // Afin d'envoyer le time-out
                        Dictionary<string, string> orderIds = new Dictionary<string, string>();
                        foreach (Dictionary<string, string> val in resultSelectPartnersId)
                        {
                            var socketId = val["socket_id"];
                            var orderId = val["order_id"];
                            var clientId = val["client_id"];
                            var socketPartner = SocketPool.FirstOrDefault(x => x.Key == socketId).Value;
                            var data = new { action = "time_out", order = orderId };
                            var json_to_send = data;
                            var v = await SocketWriteSmart(socketPartner, data);
                            Program.MainForm.WriteTextOnConsole("Time Out send to " + socketId);
                            orderIds[orderId] = clientId;
                        }

                        if (orderIds != null)
                        {
                            foreach (var val in orderIds)
                            {
                                var mySqlRequest = new MySqlRequest();
                                // Deuxieme selection (vue avec eri (03/06/19) si apres deuxieme selection rien, on fait ca)
                                // Suprtession de la commande dans la database
                                // dans le cas d'une seconde selection, on supprime la premiere commande saisie dans la base de données.
                                var sql_delete_order = "DELETE FROM confirmation WHERE Id = '" + val.Key + "';";
                                if (!mySqlRequest.ExecuteNonQuery(sql_delete_order)) { Program.MainForm.WriteTextOnConsole("DELETE ORDER Mysql Error"); }
                                var data2 = new { action = "time_out" };
                                var json_to_send = data2;
                                var numSocketClient = "C" + val.Value;
                                var socketClient = SocketPool.FirstOrDefault(x => x.Key == numSocketClient).Value;
                                var w = await SocketWriteSmart(socketClient, json_to_send);
                            }
                        }

                        // Effacer toutes les entrées supperieure a 30 sec;
                        var sql_delete = "DELETE FROM selection WHERE time_stamp <=  '" + needToBeDeleteStr + "'";

                        if (sqlRequest.ExecuteNonQuery(sql_delete) == false) { Program.MainForm.WriteTextOnConsole("CleanUpSelectionTable : DELETE Mysql Error"); }

                        if (t > 0)
                        {
                            Thread.Sleep(t * 1000);
                            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + ":" + t.ToString() + "s");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Ecriture sur base de donné en cas de deconnection socket
        /// </summary>
        /// <param name="client"></param>
        /// <param name="type"></param>
        /// <param name="id"></param>
        private void DisconnectSocket(ITcpSocketClient client, string type, string id)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();
            var sqlDeleteIdsocket = string.Empty;

            if (type == "P")
            {
                sqlDeleteIdsocket = "UPDATE partners SET socket_id = NULL,online=0,available=0 WHERE Id = " + id + ";";
            }
            else if (type == "C")
            {
                sqlDeleteIdsocket = "UPDATE client SET socket_id = NULL WHERE Id_client = " + id + ";";
            }
            //if (!($result = $mysqli->query($sql_delete_idsocket))) throw new Exeption("La variable 'sql_delete_socket' est vide");
            if (sqlDeleteIdsocket != string.Empty)
            {
                if (mySqlRequest.ExecuteNonQuery(sqlDeleteIdsocket) == false) { Program.MainForm.WriteTextOnConsole("mySqlRequestDeletSocket error : " + sqlDeleteIdsocket); }
            }
        }

        /// <summary>
        /// Listener de connection entrante
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        private async void Listener_ConnectionReceived(object sender, Sockets.Plugin.Abstractions.TcpSocketListenerConnectEventArgs args)
        {
            var client = args.SocketClient;

            var test = await client.GetConnectedInterfaceAsync();

            var bytesRead = -1;
            var buf = new byte[2048];

            while (bytesRead != 0)
            {
                try
                {
                    bytesRead = await args.SocketClient.ReadStream.ReadAsync(buf, 0, buf.Length);

                    if (bytesRead > 0)
                    {
                        Console.WriteLine(ASCIIEncoding.UTF8.GetString(buf));
                        string value = ASCIIEncoding.UTF8.GetString(buf);

                        Array.Clear(buf, 0, buf.Length);
                        // Affiche les logs de toutes les data qui transit.
                        var culture = new CultureInfo("fr-FR");
                        DateTime localDate = DateTime.Now;
                        //DateTime utcDate = DateTime.UtcNow; // Decomenter pour avoir une date UTC
                        // Program.MainForm.WriteTextOnConsole(localDate.ToString(culture) + " : " + value.ToString());
                        Console.WriteLine(localDate.ToString(culture) + " : " + value);

                        JObject jsonVal = new JObject();
                        try
                        {
                            jsonVal = JObject.Parse(value);
                            // monter u tableau de json 
                            // pour chaque json si ca contien faire action 
                            // effacer la ligne executé 
                        }
                        catch (Exception exeptionSocket)
                        {
                            Program.MainForm.WriteTextOnConsole("!!!!!!!!!!!!!" + value);
                        }

                        //JsonValue jsonValue_with_action = JsonValue.Parse(value.Trim());
                        //jsonValue = jsonValue_with_action["data"];

                        IList<string> keys = jsonVal.Properties().Select(p => p.Name).ToList();

                        if (keys.Contains("transnum"))
                        {
                            var transactionNumber = jsonVal["transnum"];
                            var response = new { transnum = transactionNumber };
                            var bck = await SocketWriteSmart(client, response);
                            if (bck.Result)
                            {
                                Program.MainForm.WriteTextOnConsole("->transnum:" + transactionNumber.ToString());
                            }
                            else
                            {
                                Program.MainForm.WriteTextOnConsole("->transnum:ERROR (" + bck.ExceptionDetails.Message + ")");
                            }

                        }

                        if (keys.Contains("action"))
                        {
                            if (jsonVal["action"].ToString() == "identification")
                            {
                                //
                                // Il faut suspendre tout action Send du temps que l'opération identification soit accomplie
                                // 

                                IdentificationBool = false;

                                var id = jsonVal["id"].ToString();
                                var type = jsonVal["type"].ToString();
                                var token = jsonVal["token"].ToString();
                                var os = jsonVal["os"].ToString();

                                if (SocketPool.ContainsKey(type + id))
                                {
                                    //var aaa = SocketPool.First(x => x.Value == client).Value;

                                    //if (aaa==null)
                                    //{
                                    var socketId = type + id;
                                    var socketToDispose = SocketPool.FirstOrDefault(x => x.Key == (type + id)).Value;
                                    SocketPool.Remove(type + id);

                                    DisconnectSocket(socketToDispose, "C", id);
                                    socketToDispose.Dispose();
                                    Program.MainForm.RemoveClientRow(id);
                                    //}

                                    //DisconnectSocket(client, "C", socketId);
                                    //DisconnectSocket(client, "C", id);

                                    // Reconnection du socket
                                    var toto = await client.GetConnectedInterfaceAsync();

                                    SocketPool.Add(type + id, client);
                                    try
                                    {
                                        //   TokenPool.Add(type + id, token);
                                    }
                                    catch (Exception poolError)
                                    {
                                        Console.WriteLine("ERREUR INSERTION DANS SOCKET PULL" + poolError.Message);
                                    }

                                }
                                else
                                {
                                    SocketPool.Add(type + id, client);
                                    try
                                    {
                                        TokenPool.Add(type + id, token);
                                    }
                                    catch { Console.WriteLine("ERREUR XX2 INSERTION DANS SOCKET PULL"); }

                                }

                                IdentificationBool = true;

                                ActionForIdentification(id, type, client, os);

                                //test only
                                //Program.MainForm.AddPartnerRow(id, client.RemoteAddress, "Connected");
                                //test only
                            }
                            if (jsonVal["action"].ToString() == "send_notif")
                            {
                                var socketId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                ActionForSendNotification(client, jsonVal);
                            }
                            if (jsonVal["action"].ToString() == "order")
                            {
                                //var clientId = SocketPool.FirstOrDefault(x => x.Value.Equals(client)).Key;
                                ActionForOrder("", "", client, jsonVal);

                            }
                            if (jsonVal["action"].ToString() == "accept")
                            {
                                var partnerId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                ActionForAccept(jsonVal, partnerId);
                            }
                            if (jsonVal["action"].ToString() == "order_refuse")
                            {
                                var partnerId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                ActionForOrderRefuse(jsonVal, partnerId);
                            }
                            if (jsonVal["action"].ToString() == "cancel")
                            {
                                var socketId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                var partnerId = socketId.Replace("P", string.Empty);
                                ActionForCancelOrder(client, jsonVal, partnerId);
                            }
                            if (jsonVal["action"].ToString() == "client_cancel_order")
                            {
                                ActionForClientCancelOrder(client);
                            }
                            if (jsonVal["action"].ToString() == "update_partner_position")
                            {
                                var socketId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                var partnerId = socketId.Replace("P", string.Empty);
                                ActionForUpdatePartner(jsonVal, socketId, partnerId);
                            }
                            if (jsonVal["action"].ToString() == "update_client_position")
                            {
                                var socketId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                var clientId = socketId.Replace("C", string.Empty);
                                ActionForUpdateClient(jsonVal, socketId, clientId);
                            }
                            if (jsonVal["action"].ToString() == "client_approched")
                            {
                                ActionForClientApproched(jsonVal);
                            }
                            if (jsonVal["action"].ToString() == "no_customer")
                            {
                                ActionForNoCustomer(jsonVal);
                            }
                            if (jsonVal["action"].ToString() == "client_taken")
                            {
                                ActionForClientTaken(jsonVal, client);
                            }
                            if (jsonVal["action"].ToString() == "release_client")
                            {
                                var socketId = SocketPool.FirstOrDefault(x => x.Value == client).Key;
                                var partnerId = socketId.Replace("P", string.Empty);
                                ActionForReleaseClient(jsonVal, partnerId);
                            }

                        }
                    }
                }
                catch (Exception e)
                {
                    if (e.HResult == -2146232798)
                    {
                        //deconnection server
                        //Program.MainForm.WriteTextOnConsole("Client disconnected " + client.RemoteAddress.ToString());
                        break;
                    }
                    else if (e.HResult == -2146233079)
                    {
                        //deconection Client
                        //Program.MainForm.WriteTextOnConsole("Client disconnected " + client.RemoteAddress.ToString());
                        break;
                    }
                    else
                    {
                        var error = e.GetBaseException();
                        //Program.MainForm.WriteTextOnConsole(e.Message);
                        Program.MainForm.WriteTextOnConsole("==============================================");
                        Program.MainForm.WriteTextOnConsole(error.Message);
                        Program.MainForm.WriteTextOnConsole("==================STACKTRACE==================");
                        Program.MainForm.WriteTextOnConsole(error.StackTrace);
                        Program.MainForm.WriteTextOnConsole("==============================================");
                    }
                }
            }

            //TODO : la variable socketIdToDelete est null
            var socketIdToDelete = string.Empty;
            try
            {
                socketIdToDelete = SocketPool.FirstOrDefault(x => x.Value == client).Key;
            }
            catch { }

            if (socketIdToDelete != null && socketIdToDelete != string.Empty)
            {
                var clientIdToDelete = socketIdToDelete.Remove(0, 1);
                SocketPool.Remove(socketIdToDelete);


                // test
                /*
                PushNotification pushNotification = new PushNotification(PushNotificationType.Development, ApnCerFilepath, ApnCerPwd);
                PushNotificationPayload payload = new PushNotificationPayload();
                payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == socketIdToDelete).Value;
                payload.badge = 0;
                payload.sound = "default";
                payload.message = "Deconection Test.";
                pushNotification.Push(payload);
                */
                //eof test

                //TokenPool.Remove(socketIdToDelete);

                if (socketIdToDelete.Contains("P"))
                {
                    Program.MainForm.RemovePartnerRow(clientIdToDelete);
                    DisconnectSocket(client, "P", clientIdToDelete);
                }
                else
                {
                    // test
                    /*
                    PushNotification pushNotification = new PushNotification(PushNotificationType.Development, ApnCerFinalFilepath, ApnCerFinalPwd);
                    PushNotificationPayload payload = new PushNotificationPayload();
                    payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == socketIdToDelete).Value;
                    payload.badge = 0;
                    payload.sound = "default";
                    payload.message = "Deconection Test.";
                    pushNotification.Push(payload);
                    */
                    //eof test


                    Program.MainForm.RemoveClientRow(clientIdToDelete);
                    DisconnectSocket(client, "C", clientIdToDelete);
                }
            }

        }

        /// <summary>
        /// Fonction permettant d'envoyer des notification au devices
        /// </summary>
        /// <param name="jsonVal"></param>
        private void ActionForSendNotification(ITcpSocketClient socket, JObject jsonVal)
        {
            var idSocket = jsonVal["id_socket"].ToString();
            var msg = jsonVal["msg"].ToString();
            // On lance l action dans un autre thread sinon le thread de
            // socket sleep et devien inactif du coup on ne peux plus envoyer de message.
            Task.Run(() => { SendClientRemoteNotification(idSocket, msg); });

        }

        /// <summary>
        /// Action for the "Identification" Keyword
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="socket"></param>
        private async void ActionForIdentification(string id, string type, ITcpSocketClient socket, string system = "")
        {
            if (type == "P")
            {
                Program.MainForm.AddPartnerRow(id, socket.RemoteAddress, "Connected");
                MySqlRequest mySqlRequest = new MySqlRequest();

                // on verifie sur la database si un socket n'a pas deja ete ouvert par le client et si oui on le ferme
                /*
				var sql_verify_partner_socket = "SELECT * FROM partners WHERE Id = "+id+";";
                var result_verify_vehicule = mySqlRequest.Select(sql_verify_partner_socket);
                var value = result_verify_vehicule[0];
                if (value["socket_id"]!= null )
				{
                    socket.DisconnectAsync();
                }
                */

                // si non on cree un nouveu socket sur la database

                // Pour savoir quelle Os est utilisé (nécessaire pour le webRtc)

                int os;

                if (system == "apple")
                {
                    os = 0;
                }
                else
                {
                    os = 1;
                }

                var sql_update_idsocket = string.Empty;
                // si on a son token on le stock aussi

                sql_update_idsocket = "UPDATE partners SET socket_id = '" + "P" + id + "',online=1,available=1,device_type='" + os + "' WHERE Id = " + id + ";";


                if (!mySqlRequest.ExecuteNonQuery(sql_update_idsocket))
                {
                    Program.MainForm.WriteTextOnConsole("mySQl Insert Error");
                }

                Program.MainForm.WriteTextOnConsole("Type Partner ");

                //verification que le compte est valide
                //var sql_account_is_active = "SELECT active FROM partners WHERE Id =" + id + ";";
                var sql_account_is_active = "SELECT activated,profile_full FROM partners WHERE Id =" + id + ";";


                var partnerVal = mySqlRequest.Select(sql_account_is_active);
                if (partnerVal == null)
                {
                    Program.MainForm.WriteTextOnConsole("Mysql Error2");
                }
                else
                {
                    var value_is_active = partnerVal[0];

                    if (value_is_active["activated"] == "True")
                    {
                        // verification que l'etat du partner avant de se connecter.
                        var sql_partner_state = "SELECT id_vehicule,confirmation.`status` FROM confirmation WHERE confirmation.id_vehicule = '" + id + "' AND status >0 AND status <4;";
                        var value_partner_state = new Dictionary<string, string>();
                        var result_is_active = mySqlRequest.Select(sql_partner_state);
                        if (result_is_active == null)
                        {
                            Program.MainForm.WriteTextOnConsole("Mysql Error 3");
                        }
                        else
                        {
                            if (result_is_active.Count != 0)
                            {
                                value_partner_state = result_is_active[0];
                            }

                        }

                        //print_r( $value_partner_state);//debug
                        if (result_is_active.Count > 0)
                        {
                            // Si le partenaire a une commande en cours on lui renvoie les information de la commande

                            var sql_order = "SELECT * FROM confirmation LEFT JOIN client ON (confirmation.id_client = client.Id_client) WHERE status >0 AND status <4 AND id_vehicule='" + id + "'";
                            var result_sql_order = mySqlRequest.Select(sql_order);
                            var value_order = result_sql_order[0];

                            // Recuperation des information pour le billet collectif
                            var sql_collective_Ticket = "";
                            sql_collective_Ticket += "SELECT order_ts,taken_ts, client.first_name as cliFName,client.last_name as cliLName,partners.first_name as partFName,partners.last_name as partLName FROM confirmation";
                            sql_collective_Ticket += " LEFT JOIN partners ON(partners.Id = confirmation.id_vehicule)";
                            sql_collective_Ticket += " LEFT JOIN client ON(client.Id_client = confirmation.id_client)";
                            sql_collective_Ticket += " WHERE confirmation.Id = " + value_order["Id"] + ";";

                            /*
                            sql_collective_Ticket += "SELECT order_ts, taken_ts, client.first_name as cliFName,client.last_name as cliLName,partners.first_name as partFName,";
                            sql_collective_Ticket += "partners.last_name as partLName,prise_en_charge,prix_au_km,prix_par_min FROM confirmation ";
                            sql_collective_Ticket += "LEFT JOIN partners ON(partners.Id = confirmation.id_vehicule) ";
                            sql_collective_Ticket += "LEFT JOIN client ON(client.Id_client = confirmation.id_client) ";
                            sql_collective_Ticket += "LEFT JOIN tarification ON(tarification.id = partners.type) ";
                            sql_collective_Ticket += "WHERE confirmation.Id = " + value_order["Id"] + ";";
                            */

                            var result_ticket = mySqlRequest.Select(sql_collective_Ticket);

                            if (result_ticket == null)
                            {
                                Program.MainForm.WriteTextOnConsole("Mysql Error 4");
                            }
                            var collective_tiket = result_ticket[0];

                            var partner_name = collective_tiket["partFName"] + " " + collective_tiket["partLName"];
                            var client_name = collective_tiket["cliFName"] + " " + collective_tiket["cliLName"];
                            var order_tsVal = collective_tiket["order_ts"];
                            var taken_tsVal = collective_tiket["taken_ts"];


                            var data_order = new
                            {
                                addr = value_order["pickup_addr"],
                                lat_cli = value_order["lat_cli"],
                                lon_cli = value_order["lon_cli"],
                                order = value_order["Id"],
                                des_addr = value_order["des_addr"],
                                des_lat = value_order["des_lat"],
                                des_lon = value_order["des_lon"],
                                des_price = value_order["des_price"],
                                id_client = value_order["id_client"],
                                socket_client = "C" + value_order["id_client"],
                                part_name = partner_name,
                                cli_name = client_name,
                                order_ts = order_tsVal,
                                taken_ts = taken_tsVal
                            };


                            var statusPartner = value_partner_state["status"];
                            //var data = array("action"=>"connection_result", "status"=>$status, "data_order"=>$data_order);
                            var data = new { action = "connection_result", status = statusPartner, data_order = data_order };
                            var dataJson = JsonConvert.SerializeObject(data);

                            var x = await SocketWriteSmart(socket, data);
                        }
                        else
                        {
                            //$data = array("action"=>"connection_result","state"=>"","data_order"=>$data_order);

                        }
                    }
                    else
                    {
                        var data = new { action = "not_ready" };
                        var x = await SocketWriteSmart(socket, data);
                    }

                }

                //test
                /*
                var idSocketClientFinal = "P"+id;
                var message = "push msg test.";
                string filepath = ApnCerFilepath;
                string pwd = ApnCerPwd;
                PushNotification pushNotification = new PushNotification(PushNotificationType.Development, filepath, pwd);
                PushNotificationPayload payload = new PushNotificationPayload();
                payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value;
                payload.badge = 0;
                payload.sound = "default";
                payload.message = message;
                pushNotification.Push(payload);
                */
                //eof test

            }
            else if (type == "C")
            {
                // §§§§§§§§ ERREUR 
                // System.ObjectDisposedException : 'Impossible d'accéder à un objet supprimé.
                // Nom de l'objet : 'System.Net.Sockets.Socket'.'


                Program.MainForm.AddClientRow(id, socket.RemoteAddress, "Connected");
                MySqlRequest mySqlRequest = new MySqlRequest();
                // Connection client finale

                // Enregistrement des parametre de connection dans la base de données.
                var socketId = type + id;

                // Pour savoir quelle Os est utilisé (nécessaire pour le webRtc)

                int os;

                if (system == "apple")
                {
                    os = 0;
                }
                else
                {
                    os = 1;
                }

                var sql_update_idsocket = string.Empty;

                sql_update_idsocket = "UPDATE client SET socket_id = '" + socketId + "',device_type='" + os + "' WHERE Id_client = " + id + ";";

                //if (!(result_update_idsocket = $mysqli->query($sql_update_idsocket))) exit("INSERT Mysql Error");

                var result_update_idsocket = mySqlRequest.ExecuteNonQuery(sql_update_idsocket);
                if (result_update_idsocket == false)
                {
                    Program.MainForm.WriteTextOnConsole("INSERT Mysql Error");
                }

                // Interogation du compte client pour determiné si on possede la source de sa carte banqaire

                var idClient = id;
                var sql_client_state = "SELECT stripe_token_card FROM client WHERE Id_client = '" + idClient + "';";

                var result_client_state = mySqlRequest.Select(sql_client_state);

                if (result_client_state == null)
                {
                    Program.MainForm.WriteTextOnConsole("MysqlError");
                }

                var stripeCardToken = result_client_state[0]["stripe_token_card"];

                // Récupération des informations 
                // de la carte avec l'API Stripe.
                if (stripeCardToken != "")
                {
                    StripeConfiguration.SetApiKey("sk_test_dBR2X5yRuQklppKxRz7jCbuT");
                    var sourceServie = new SourceService();

                    var source = sourceServie.Get(stripeCardToken);
                    var cardDetails = source.Card;
                    var brand = cardDetails.Brand;
                    var last4 = cardDetails.Last4;

                    // Insert des informations dans la base de données;
                    var sqlInsertDataCard = "UPDATE client SET card_type = '" + brand + "', last_card_number = '" + last4 + "' WHERE Id_client='" + idClient + "';";

                    if (mySqlRequest.ExecuteNonQuery(sqlInsertDataCard) == false)
                    {
                        Program.MainForm.WriteTextOnConsole("Insert Cartd Details mySqlRequest error");
                        Program.MainForm.WriteTextOnConsole("Last4Digit : " + last4 + " - cardBrand : " + brand);
                    }
                }

                Program.MainForm.WriteTextOnConsole("Type Client");
                // On interroge la base de données pour savoir si le client finale avais une commande en cours.
                var sql_partner_state = "SELECT id_client,confirmation.`status` FROM confirmation WHERE confirmation.id_client = '" + id + "' AND status >0 AND status <4;";

                //if (!($result_partner_state = $mysqli->query($sql_partner_state))) exit("Mysql Error");
                //$value_partner_state = $result_partner_state->fetch_object();

                var result_partner_state = mySqlRequest.Select(sql_partner_state);

                if (result_partner_state == null)
                {
                    Program.MainForm.WriteTextOnConsole("MysqlError");
                }

                // Envoie des tarifs avec les info de connection.
                var priceObj = GetTarifs();

                if (result_partner_state.Count > 0)
                {

                    var sql_order = "SELECT *,partners.Id as partner_id,confirmation.Id as order_id FROM confirmation INNER JOIN partners ON confirmation.id_vehicule = partners.Id WHERE status >0 AND status <4 AND id_client='" + id + "'";

                    var result_sql_order = mySqlRequest.Select(sql_order);
                    var value_order = result_sql_order[0];

                    string latCli = value_order["lat_cli"];
                    latCli = latCli.Replace(".", ",");

                    var lonCli = value_order["lon_cli"];
                    lonCli = lonCli.Replace(".", ",");

                    var latCliDbl = double.Parse(latCli);
                    var lonCliDbl = double.Parse(lonCli);

                    var latitudeDbl = double.Parse(value_order["latitude"]);
                    var longitudeDbl = double.Parse(value_order["longitude"]);

                    var distanceVal = Math.Round(GetDistance(latCliDbl, lonCliDbl, latitudeDbl, longitudeDbl) / 1000, 3);
                    //var distance = round(get_distance_m($latCli,$lonCli,$value_order->latitude,$value_order->longitude) / 1000, 3);


                    var data_orderVal = new
                    {
                        id_vehicule = value_order["partner_id"],
                        addr = value_order["pickup_addr"],
                        lat_cli = value_order["lat_cli"],
                        lon_cli = value_order["lon_cli"],
                        order = value_order["order_id"],
                        des_addr = value_order["des_addr"],
                        des_lat = value_order["des_lat"],
                        des_lon = value_order["des_lon"],
                        des_price = value_order["des_price"],
                        id_client = value_order["id_client"]
                    };

                    var data_partner = new
                    {
                        Id = value_order["partner_id"],
                        type = value_order["type"],
                        capacity = value_order["capacity"],
                        latitude = value_order["latitude"],
                        longitude = value_order["longitude"],
                        phone = value_order["phone"],
                        //'brand'=>$value_order->brand_name,
                        brand = value_order["brand"],
                        score = value_order["score"],
                        first = value_order["first_name"],
                        last = value_order["last_name"],
                        //'service'=>utf8_encode($service_json), // pour les caractere accentué du json
                        //'transationstatus'=>$transationstatus,
                        distance = distanceVal
                    };

                    var statusVal = value_order["status"];
                    var dataObj = new { action = "connection_result", status = statusVal, data_order = data_orderVal, data = data_partner, prices = priceObj };

                    var x = await SocketWriteSmart(socket, dataObj);

                }
                else
                {
                    var data = new { action = "connection_result", status = "0", prices = priceObj };
                    var x = await SocketWriteSmart(socket, data);
                }

                //test
                /*
                //Console.WriteLine("start to sleep");
                //Thread.Sleep(20000);
                var idSocketClientFinal = socketId;
                var message = "Hello is it the good text.";
                string filepath = ApnCerFinalFilepath;
                string pwd = ApnCerFinalPwd;
                PushNotification pushNotification = new PushNotification(PushNotificationType.Development, filepath, pwd);
                PushNotificationPayload payload = new PushNotificationPayload();
                payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value;
                payload.badge = 0;
                payload.sound = "default";
                payload.message = message;
                pushNotification.Push(payload);
                */
                //eof test

            }
        }

        /// <summary>
        /// Action for the "Update" keyword
        /// </summary>
        /// <param name="jsonVal"></param>
        /// <param name="socketId"></param>
        /// <param name="partnerId"></param>
        private async void ActionForUpdatePartner(JObject jsonVal, string socketId, string partnerId)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();

            //TODO : si le socket client n existe pas verifier l'id du client finale
            var lat = jsonVal["lat"];
            var lon = jsonVal["lon"];
            var rot = jsonVal["rot"];

            if (!jsonVal.ContainsKey("id_client"))
            //if (jsonVal["id_client"].ToString() == string.Empty)            
            {
                // On reformate les données pour mySqlRequest
                var latSql = lat.ToString().Replace(",", ".");
                var lonSql = lon.ToString().Replace(",", ".");

                var sql_update_partner_pos = "UPDATE partners SET latitude = " + latSql + ",longitude=" + lonSql + ",icon_rotation=" + rot.ToString() + " WHERE Id = " + partnerId + ";";

                if (mySqlRequest.ExecuteNonQuery(sql_update_partner_pos) == false) { Program.MainForm.WriteTextOnConsole("INSERT Mysql Error : " + sql_update_partner_pos); }

            }
            else
            {
                var data = new
                {
                    action = "update_partner_position",
                    lat = lat.ToString(),
                    lon = lon.ToString(),
                    rot = rot.ToString()
                };

                var idClientFinal = jsonVal["id_client"];
                //var idSocketClientFinal = "C" + idClientFinal.ToString();
                var idSocketClientFinal = "C" + jsonVal["id_client"].ToString();

                if (SocketPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value != null)
                {
                    var socketClientFinal = SocketPool[idSocketClientFinal];
                    var x = await SocketWriteSmart(socketClientFinal, data);
                }
            }
        }

        /// <summary>
        /// Action for the "Update" keyword
        /// </summary>
        /// <param name="jsonVal"></param>
        /// <param name="socketId"></param>
        /// <param name="clientId"></param>
        private void ActionForUpdateClient(JObject jsonVal, string socketId, string clientId)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();

            //TODO : si le socket client n existe pas verifier l'id du client finale
            var lat = jsonVal["lat"];
            var lon = jsonVal["lon"];
            var rot = jsonVal["rot"];

            // On reformate les données pour mySqlRequest
            var latSql = lat.ToString().Replace(",", ".");
            var lonSql = lon.ToString().Replace(",", ".");

            var sql_update_client_pos = "UPDATE client SET latitude = " + latSql + ",longitude=" + lonSql + " WHERE Id_client = " + clientId + ";";

            if (mySqlRequest.ExecuteNonQuery(sql_update_client_pos) == false) { Program.MainForm.WriteTextOnConsole("INSERT Mysql Error : " + sql_update_client_pos); }

            ActionForClientUpdatePosition(jsonVal, socketId, clientId);

        }

        private async void ActionForClientUpdatePosition(JObject jsonVal, string socketId, string clientId)
        {
            // Fonction responsable de la transmition des position partner pour affiché les partner dans la zone sur la carte.
            string lat = jsonVal["lat"].ToString();
            string lon = jsonVal["lon"].ToString();

            // Initialisation de variables
            MySqlRequest mySqlRequest = new MySqlRequest();
            var idSocketClientFinal = socketId;




            ITcpSocketClient socketCliFinale = null;



            // Calcule de la zone de selection
            var zone_de_selection = 50; // en kilometre

            //if (jsonVal["attempt"].ToString() == "1")
            //{
            //    zone_de_selection = 100;
            //}

            var km_lon = 111.11 * Math.Cos(double.Parse(lat.Replace(".", ",")));// nombre de kilometre pour 1° de longitude en fonction de la latitude
            var add_to_lon = (km_lon / zone_de_selection) / 2;
            var add_to_lat = (111.11 / zone_de_selection) / 2;

            var latitudeMin = double.Parse(lat.Replace(".", ",")) - add_to_lat;
            var latitudeMax = double.Parse(lat.Replace(".", ",")) + add_to_lat;
            var longitudeMin = double.Parse(lon.Replace(".", ",")) - add_to_lon;
            var longitudeMax = double.Parse(lon.Replace(".", ",")) + add_to_lon;

            // Sélection des véhicule disponnible et envoie de la commande au véhicules sélectionés.

            var sql_vehicule = "";
            sql_vehicule += "SELECT  latitude, longitude, icon_rotation FROM partners ";
            sql_vehicule += "WHERE online = 1 AND activated = 1 AND latitude BETWEEN " + latitudeMin.ToString().Replace(",", ".") + " AND " + latitudeMax.ToString().Replace(",", ".");
            sql_vehicule += "AND longitude BETWEEN " + longitudeMin.ToString().Replace(",", ".") + " AND " + longitudeMax.ToString().Replace(",", ".") + " LIMIT 5; ";

            var result_vehicule = mySqlRequest.Select(sql_vehicule);

            if (result_vehicule == null) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + sql_vehicule); }

            //   Program.MainForm.WriteTextOnConsole(sql_vehicule);


            if (result_vehicule.Count >= 0)
            {
                var n = result_vehicule.Count;
                /*
                string[,] arrayPartners = new string[n, 3];

                for (var i = 0; i < n; i++)
                { 
                    arrayPartners[i, 0] = result_vehicule[i]["latitude"];
                    arrayPartners[i, 1] = result_vehicule[i]["longitude"];
                    arrayPartners[i, 2] = result_vehicule[i]["icon_rotation"];
                }

                List<object> list = arrayPartners.Cast<Object>().ToList();
                var jsonObj = JsonConvert.SerializeObject(list);
                */

                List<List<string>> partnersList = new List<List<string>>();
                for (var i = 0; i < n; i++)
                {
                    var tmpList = new List<string>();

                    tmpList.Add(result_vehicule[i]["latitude"]);
                    tmpList.Add(result_vehicule[i]["longitude"]);
                    tmpList.Add(result_vehicule[i]["icon_rotation"]);

                    partnersList.Add(tmpList);
                }

                var dataPartners = JsonConvert.SerializeObject(partnersList);

                var data = new { action = "listpartner", data_partners = dataPartners };

                if (SocketPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value != null)
                {
                    var socketClientFinal = SocketPool[idSocketClientFinal];
                    var x = await SocketWriteSmart(socketClientFinal, data);
                }
            }


        }


        /// <summary>
        /// Fonction qui gére l'annulation des commande par le partenaire.
        /// Elle refait une séléction et renvoie un nouveau partenaire au client.
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="jsonVal"></param>
        /// <param name="partnerId"></param>
        private void ActionForCancelOrder(ITcpSocketClient socket, JObject jsonVal, string partnerId)
        {
            // En cas d'annulation on execute une surcharge de la fonction ActionForOrder
            ActionForOrder("", "", socket, jsonVal, true);

        }

        /// <summary>
        /// Fonction qui gere les annulation client finale.
        /// </summary>
        /// <param name="socket"></param>
        private async void ActionForClientCancelOrder(ITcpSocketClient socket)
        {
            // On retrouver l id du client par le biais de son socket.
            var socketId = SocketPool.FirstOrDefault(x => x.Value == socket).Key;
            var clientId = socketId.Replace("C", string.Empty);

            //Archivage de la commande et update du payment Intent et transfer chez stripe
            var transferParam = StripeOperationCancelOrder(clientId);

            // Archivage de la commande.
            MySqlRequest mySqlRequest = new MySqlRequest();
            //            var selectInfoOrder = "SELECT confirmation.Id AS orderId,partners.Id AS partnerId FROM confirmation WHERE confirmation.id_client = '" + clientId + "';";
            var selectInfoOrder = "SELECT confirmation.Id AS orderId,id_vehicule AS partnerId FROM confirmation WHERE confirmation.id_client = '" + clientId + "';";

            var resultInfoOrder = mySqlRequest.Select(selectInfoOrder);
            if (resultInfoOrder == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + selectInfoOrder); }

            var orderId = resultInfoOrder[0]["orderId"];
            var partnerId = resultInfoOrder[0]["partnerId"];

            ArchiveOrder(orderId.ToString(), "5", string.Empty, transferParam);

            // Recuperation du montant de l annulation dans la base de donnée.
            // Recuperation des données de la commande
            var sql_select_cancelfee = "SELECT driver_net_price FROM orders_terminated WHERE orders_terminated.Id = '" + orderId + "' ";
            var result_select_cancelfee = mySqlRequest.Select(sql_select_cancelfee);
            if (result_select_cancelfee == null) { Program.MainForm.WriteTextOnConsole("mysql Error"); }
            var cancelFee = result_select_cancelfee[0]["driver_net_price"];

            // Envoie d'un message au partneaire pour l'informer que la commande est annulé par le client.
            var data = new { action = "client_cancel_order", data_cancel = cancelFee };

            var json_to_send = data;

            if (SocketPool.FirstOrDefault(x => x.Key == "P" + partnerId).Value != null)
            {
                var partnerSocket = SocketPool.FirstOrDefault(x => x.Key == "P" + partnerId).Value;
                var t = await SocketWriteSmart(partnerSocket, data);
            }

        }

        /// <summary>
        /// Surchage utilisée dans la fonction ActionForReleaseClient
        /// </summary>
        /// <param name="jsonFile"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        private string ArchiveOrder(JObject jsonFile, List<string> transferParam, string status = "4")
        {

            string orderId = jsonFile["order"].ToString();
            string releaseAddr = jsonFile["addr"].ToString();

            var itemToReturn = ArchiveOrder(orderId, status, releaseAddr, transferParam);
            return itemToReturn;
        }

        /// <summary>
        /// Surchage utilisé dans les fonctions ActionForNoCustomer et ActionForClientCancelOrder
        /// </summary>
        /// <param name="orderId"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        private string ArchiveOrder(string orderId, string status = "4")
        {
            var itemToReturn = ArchiveOrder(orderId, status, "");
            return itemToReturn;
        }

        /// <summary>
        /// Fonction responsable de l'archivage des info de commande contenue 
        /// dans la table confirmation et transmise a la table order_terminated
        /// </summary>
        /// <param name="orderId"></param>
        /// <param name="status"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        private string ArchiveOrder(string orderId, string status, string address = "", List<string> transferParam = null)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();

            // Mise a jour de l etat de la commande au cas ou le partenaire aurai une perte de connection.
            var sql_update_order = "UPDATE confirmation SET status='" + status + "',delivered_ts=CURRENT_TIMESTAMP,delivered_addr='" + address + "' WHERE  Id='" + orderId + "';";
            if (mySqlRequest.ExecuteNonQuery(sql_update_order) == false) { Program.MainForm.WriteTextOnConsole("Mysql Error sql_update_order"); }

            // Recuperation des données de la commande
            var sql_select_confirmation = "SELECT * FROM confirmation LEFT JOIN client ON (client.Id_client = confirmation.id_client) WHERE confirmation.Id = '" + orderId + "' ";
            var result_select_confirmation = mySqlRequest.Select(sql_select_confirmation);
            if (result_select_confirmation == null) { Program.MainForm.WriteTextOnConsole("mysql Error"); }
            var val = result_select_confirmation[0];
            //on passe les data au bon format pour insertion mySqlRequest
            var pickup_arrived_ts = "";
            if (val["pickup_arrived_ts"] == "") { pickup_arrived_ts = "NULL"; } else { pickup_arrived_ts = "'" + DateTime.Parse(val["pickup_arrived_ts"].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "'"; }

            var order_ts = "";
            if (val["order_ts"] == "") { order_ts = "NULL"; } else { order_ts = "'" + DateTime.Parse(val["order_ts"].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "'"; }

            var taken_ts = "";
            if (val["taken_ts"] == "") { taken_ts = "NULL"; } else { taken_ts = "'" + DateTime.Parse(val["taken_ts"].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "'"; }

            var delivered_ts = "";
            if (val["delivered_ts"] == "") { delivered_ts = "NULL"; } else { delivered_ts = "'" + DateTime.Parse(val["delivered_ts"].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "'"; }

            var sql_insert_archive = "";
            sql_insert_archive += "INSERT INTO orders_terminated (";
            sql_insert_archive += "Id,";
            sql_insert_archive += "id_client,";
            sql_insert_archive += "id_vehicule,";
            sql_insert_archive += "status,";
            sql_insert_archive += "order_ts,";
            sql_insert_archive += "pickup_arrived_ts,";
            sql_insert_archive += "taken_ts,";
            sql_insert_archive += "delivered_ts,";
            sql_insert_archive += "pickup_addr,";
            sql_insert_archive += "delivered_addr,";
            sql_insert_archive += "lat_cli,";
            sql_insert_archive += "lon_cli,";
            sql_insert_archive += "des_addr,";
            sql_insert_archive += "des_lat,";
            sql_insert_archive += "des_lon,";
            sql_insert_archive += "des_price,";
            sql_insert_archive += "com_stripe,";
            sql_insert_archive += "com_ct8,";
            sql_insert_archive += "driver_net_price,";
            sql_insert_archive += "part_capacity,";
            sql_insert_archive += "part_brand,";
            sql_insert_archive += "part_first_name,";
            sql_insert_archive += "part_last_name,";
            sql_insert_archive += "part_email,";
            sql_insert_archive += "part_rib,";
            sql_insert_archive += "part_phone,";
            sql_insert_archive += "cli_phone,";
            sql_insert_archive += "cli_first_name,";
            sql_insert_archive += "cli_last_name,";
            sql_insert_archive += "cli_email,";
            sql_insert_archive += "pspReference,";
            sql_insert_archive += "transfer_number,";
            sql_insert_archive += "balance_transaction,";
            sql_insert_archive += "destination_account,";
            sql_insert_archive += "destination_payment,";
            sql_insert_archive += "source_transaction,";
            sql_insert_archive += "transfer_group";
            sql_insert_archive += ") VALUES(";
            sql_insert_archive += "'" + val["Id"] + "',";
            sql_insert_archive += "'" + val["id_client"] + "',";
            sql_insert_archive += "'" + val["id_vehicule"] + "',";
            sql_insert_archive += "'" + val["status"] + "',";
            sql_insert_archive += order_ts + ",";
            sql_insert_archive += pickup_arrived_ts + ",";
            sql_insert_archive += taken_ts + ",";
            sql_insert_archive += delivered_ts + ",";
            sql_insert_archive += "'" + val["pickup_addr"] + "',";
            sql_insert_archive += "'" + val["delivered_addr"] + "',";
            sql_insert_archive += "'" + val["lat_cli"].Replace(",", ".") + "',";
            sql_insert_archive += "'" + val["lon_cli"].Replace(",", ".") + "',";
            sql_insert_archive += "'" + val["des_addr"] + "',";
            sql_insert_archive += "'" + val["des_lat"].Replace(",", ".") + "',";
            sql_insert_archive += "'" + val["des_lon"].Replace(",", ".") + "',";
            sql_insert_archive += "'" + val["des_price"].Replace(",", ".") + "',";
            sql_insert_archive += "'" + transferParam[6].Replace(",", ".") + "',";
            sql_insert_archive += "'" + transferParam[7].Replace(",", ".") + "',";
            sql_insert_archive += "'" + transferParam[8].Replace(",", ".") + "',";
            sql_insert_archive += "'" + val["part_capacity"] + "',";
            sql_insert_archive += "'" + val["part_brand"] + "',";
            sql_insert_archive += "'" + val["part_first_name"] + "',";
            sql_insert_archive += "'" + val["part_last_name"] + "',";
            sql_insert_archive += "'" + val["part_email"] + "',";
            sql_insert_archive += "'" + val["part_rib"] + "',";
            sql_insert_archive += "'" + val["part_phone"] + "',";
            sql_insert_archive += "'" + val["Id_phone"] + "',";
            sql_insert_archive += "'" + val["first_name"] + "',";
            sql_insert_archive += "'" + val["last_name"] + "',";
            sql_insert_archive += "'" + val["email"] + "',";
            sql_insert_archive += "'" + val["pspReference"] + "',";
            sql_insert_archive += "'" + transferParam[0] + "',";
            sql_insert_archive += "'" + transferParam[1] + "',";
            sql_insert_archive += "'" + transferParam[2] + "',";
            sql_insert_archive += "'" + transferParam[3] + "',";
            sql_insert_archive += "'" + transferParam[4] + "',";
            sql_insert_archive += "'" + transferParam[5] + "'";
            sql_insert_archive += ")";

            //if (!($result_insert_archive = $mysqli->query($sql_insert_archive))) exit("Mysql Error : ".$sql_insert_archive);
            if (mySqlRequest.ExecuteNonQuery(sql_insert_archive) == false)
            {
                Program.MainForm.WriteTextOnConsole("Mysql Error : " + sql_insert_archive);
            }
            //supression de la confirmation
            var sql_delete_archive = "DELETE FROM confirmation WHERE Id='" + val["Id"] + "'";
            var result_delete_archive = mySqlRequest.ExecuteNonQuery(sql_delete_archive);
            if (result_delete_archive == false) { Program.MainForm.WriteTextOnConsole("Mysql Error"); }

            return val["id_client"].ToString();
        }


        /// <summary>
        /// Action for the "order" keyword
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="socket"></param>
        /// <param name="jsonVal"></param>
        private async void ActionForOrder(string id, string type, ITcpSocketClient socket, JObject jsonVal, bool cancel = false)
        {
            // Fonction responsable de la selection des partenaires disponnibles

            // Initialisation de variables
            MySqlRequest mySqlRequest = new MySqlRequest();

            string lat;
            string lon;
            string deslat;
            string deslon;
            string price;
            string ookcom;
            string strcom;
            string partcom;
            string cat = "0";

            string pi;
            string order_id;

            string up_addr;
            string down_addr;
            string id_cli;
            string socket_client_id = "";

            ITcpSocketClient socketCliFinale = null;

            // Pour savoir si cette selection fait suite a un order cancel
            // car si on a un cancel du partner on modifie la commande au lieu de la créé.
            if (cancel)
            {

                // Insertion d'une entrée dans la base dedonnée pour garder une trace de l'annulation.
                var sql_insert_cancel = "INSERT INTO `refus_course` (`id_order`, `id_partner`, `motif_refus`, `text_mess`) VALUES ('" + jsonVal["order"] + "','" + jsonVal["id_partner"] + "' ,'" + jsonVal["motif"] + "' , '" + jsonVal["message"] + "');";
                mySqlRequest.ExecuteNonQuery(sql_insert_cancel);

                // Recuperation des information de commande sur la base de données.                       
                var sql_recovery_order_infos = "SELECT lat_cli,lon_cli,des_lat,des_lon,des_price,pspReference,pickup_addr,delivered_addr,type FROM confirmation LEFT JOIN partners ON(partners.Id = confirmation.id_vehicule) WHERE confirmation.Id=" + jsonVal["order"] + ";";
                var result_recovery = mySqlRequest.Select(sql_recovery_order_infos);

                lat = result_recovery[0]["lat_cli"].ToString();
                lon = result_recovery[0]["lon_cli"].ToString();
                deslat = result_recovery[0]["des_lat"].ToString();
                deslon = result_recovery[0]["des_lon"].ToString();
                price = result_recovery[0]["des_price"].ToString();
                pi = result_recovery[0]["pspReference"].ToString();
                cat = result_recovery[0]["type"].ToString();

                up_addr = result_recovery[0]["pickup_addr"].ToString();
                down_addr = result_recovery[0]["delivered_addr"].ToString();

                order_id = jsonVal["order"].ToString();
                id_cli = jsonVal["client"].ToString();

                // On update l'ancienne commande comme ca on a pas a recrée une commande.
                var sql_Update_Last_Order = string.Empty;
                sql_Update_Last_Order = "UPDATE confirmation SET id_vehicule = NULL, order_ts=NULL,part_capacity=NULL,part_brand = NULL,part_first_name = NULL,part_last_name = NULL,part_email = NULL,part_rib = NULL,part_phone = NULL,part_password = NULL,part_socket_id = NULL WHERE Id = " + jsonVal["order"] + ";";
                mySqlRequest.ExecuteNonQuery(sql_Update_Last_Order);

                socketCliFinale = SocketPool.FirstOrDefault(x => x.Key == "C" + id_cli.ToString()).Value;
            }
            else
            {
                // Cast des valeur numerique dans le format double de mySqlRequest

                lat = jsonVal["lat"].ToString().Replace(",", ".");
                lon = jsonVal["lon"].ToString().Replace(",", ".");
                deslat = jsonVal["deslat"].ToString().Replace(",", ".");
                deslon = jsonVal["deslon"].ToString().Replace(",", ".");
                price = jsonVal["price"].ToString().Replace(",", ".");

                /*
                ookcom = jsonVal["ookcom"].ToString().Replace(",", ".");
                strcom = jsonVal["strcom"].ToString().Replace(",", ".");
                partcom = jsonVal["partcom"].ToString().Replace(",", ".");
                */

                ookcom = string.Empty;
                strcom = string.Empty;
                partcom = string.Empty;

                cat = jsonVal["cat"].ToString().Replace(",", ".");
                int intCat = int.Parse(cat);
                intCat++; // on ajoute 1 au type car sur le client il commence a 0;
                cat = intCat.ToString();
                pi = jsonVal["pi"].ToString();
                up_addr = jsonVal["upaddr"].ToString();
                down_addr = jsonVal["desaddr"].ToString();
                id_cli = jsonVal["idclient"].ToString();
                socket_client_id = "C" + id_cli;

                // Insertion de la commande dans la base de données.

                if (jsonVal["attempt"].ToString() == "1")
                {

                    // dans le cas d'une seconde selection, on supprime la premiere commande saisie dans la base de données.
                    var sql_delete_last_order = "DELETE FROM confirmation WHERE id_client = '" + jsonVal["idclient"] + "' and id_vehicule IS NULL;";

                    if (!mySqlRequest.ExecuteNonQuery(sql_delete_last_order)) { Program.MainForm.WriteTextOnConsole("DELETE LAST ORDER Mysql Error"); }
                }
                var sql_insert = "";
                sql_insert += "INSERT INTO  confirmation (Id ,id_client ,id_vehicule ,status ,order_ts,pickup_addr,lat_cli,lon_cli,des_addr,des_lat,des_lon,des_price,com_stripe,com_ct8,driver_net_price,pspReference)";
                sql_insert += " VALUES(NULL, '" + jsonVal["idclient"] + "', NULL, '0', CURRENT_TIMESTAMP, '" + jsonVal["upaddr"] + "', '" + lat + "', '" + lon + "', '" + jsonVal["desaddr"] + "', '" + deslat + "', '" + deslon + "', '" + price + "',NULL,NULL,NULL,'" + pi + "') ";

                order_id = mySqlRequest.Insert(sql_insert);

                if (order_id == string.Empty) { Program.MainForm.WriteTextOnConsole("INSERT ORDER Mysql Error : " + sql_insert); }

            }


            // Calcule de la zone de selection
            var zone_de_selection = 50; // en kilometre

            //if (jsonVal["attempt"].ToString() == "1")
            //{
            //    zone_de_selection = 100;
            //}

            var km_lon = 111.11 * Math.Cos(double.Parse(lat.Replace(".", ",")));// nombre de kilometre pour 1° de longitude en fonction de la latitude
            var add_to_lon = (km_lon / zone_de_selection) / 2;
            var add_to_lat = (111.11 / zone_de_selection) / 2;

            var latitudeMin = double.Parse(lat.Replace(".", ",")) - add_to_lat;
            var latitudeMax = double.Parse(lat.Replace(".", ",")) + add_to_lat;
            var longitudeMin = double.Parse(lon.Replace(".", ",")) - add_to_lon;
            var longitudeMax = double.Parse(lon.Replace(".", ",")) + add_to_lon;

            // Recuperation des information pour le billet collectif

            var sql_collective_Ticket = "";
            sql_collective_Ticket += " SELECT order_ts,taken_ts, client.first_name as cliFName,client.last_name as cliLName,partners.first_name as partFName,";
            sql_collective_Ticket += " partners.last_name as partLName FROM confirmation";
            sql_collective_Ticket += " LEFT JOIN partners ON(partners.Id = confirmation.id_vehicule)";
            sql_collective_Ticket += " LEFT JOIN client ON(client.Id_client = confirmation.id_client)";
            sql_collective_Ticket += " WHERE confirmation.Id = " + order_id + ";";

            var result_ticket = mySqlRequest.Select(sql_collective_Ticket);

            if (result_ticket == null) { Program.MainForm.WriteTextOnConsole("Mysql Error : " + sql_collective_Ticket); }

            var collective_tiket = result_ticket[0];

            var partner_name = collective_tiket["partFName"] + " " + collective_tiket["partLName"];
            var client_name = collective_tiket["cliFName"] + " " + collective_tiket["cliLName"];
            var order_tsVal = collective_tiket["order_ts"];
            var taken_tsVal = collective_tiket["taken_ts"];


            // Sélection des véhicule disponnible et envoie de la commande au véhicules sélectionés.

            //$sql_vehicule = "SELECT vehicule.Id, type, capacity, latitude, longitude, score, socket_id, brand.value as brand_name FROM vehicule 
            //LEFT JOIN brand ON (vehicule.brand = brand.Id)
            //WHERE online = 1 AND available=1 AND latitude BETWEEN ".str_replace(",",".",$latitudeMin)." AND ".str_replace(",",".",$latitudeMax)." 
            //AND longitude BETWEEN ".str_replace(",",".",$longitudeMin)." AND ".str_replace(",",".",$longitudeMax)." LIMIT 5;";

            string optional_request = "";
            if (cancel == true)
            {
                optional_request = " AND partners.Id != '" + jsonVal["id_partner"] + "'";
            }


            var sql_vehicule = "";
            sql_vehicule += "SELECT partners.Id, type, expiration_date, capacity, latitude, longitude, score, partners.socket_id as socket_id,selection.order_id, brand as brand_name FROM partners ";
            sql_vehicule += "LEFT JOIN selection ON(partners.Id = selection.partner_id) ";
            sql_vehicule += "WHERE type=" + cat + " AND online = 1 AND available = 1 AND activated = 1 AND latitude BETWEEN " + latitudeMin.ToString().Replace(",", ".") + " AND " + latitudeMax.ToString().Replace(",", ".") + " AND order_id IS NULL ";
            sql_vehicule += "AND longitude BETWEEN " + longitudeMin.ToString().Replace(",", ".") + " AND " + longitudeMax.ToString().Replace(",", ".") + optional_request + " LIMIT 5; ";

            var result_vehicule = mySqlRequest.Select(sql_vehicule);

            if (result_vehicule == null) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + sql_vehicule); }

            Program.MainForm.WriteTextOnConsole(sql_vehicule);


            if (result_vehicule.Count > 0)
            {
                // Si nous avons pas asser de partner dispo, nous solisiton les partenaire en Pending
                if (result_vehicule.Count == 5)
                {
                    Program.MainForm.WriteTextOnConsole(result_vehicule.Count.ToString() + " Vehicule Séléctioner");

                    foreach (var value in result_vehicule)
                    {
                        Program.MainForm.WriteTextOnConsole("Id du partners : " + value["Id"]);
                        /*var data_orderVal = new
                        {
                            addr = jsonVal["upaddr"],
                            lat_cli = jsonVal["lat"],
                            lon_cli = jsonVal["lon"],
                            order = order_id,
                            des_addr = jsonVal["desaddr"],
                            des_lat = jsonVal["deslat"],
                            des_lon = jsonVal["deslon"],
                            des_price = jsonVal["price"],
                            id_client = jsonVal["idclient"],
                            socket_client = "0",// TODO : voir si il faut mofifier ce paramettre
                            part_name = partner_name,
                            cli_name = client_name,
                            order_ts = order_tsVal,
                            taken_ts = taken_tsVal
                        };*/

                        var data_orderVal = new
                        {
                            addr = up_addr,
                            lat_cli = lat,
                            lon_cli = lon,
                            order = order_id,
                            des_addr = down_addr,
                            des_lat = deslat,
                            des_lon = deslon,
                            des_price = price,
                            id_client = id_cli,
                            socket_client = socket_client_id,// TODO : voir si il faut mofifier ce paramettre
                            part_name = partner_name,
                            cli_name = client_name,
                            order_ts = order_tsVal,
                            taken_ts = taken_tsVal
                        };

                        /*
                        string lat;
                        string lon;
                        string deslat;
                        string deslon;
                        string price;
                        string pi;
                        string order_id;
                        string up_addr;
                        string des_addr;
                        string id_client
                        */
                        var data = new { action = "order", data_order = data_orderVal };

                        // Envoi de la commande au partenaire.
                        var SocketId = "P" + value["Id"].ToString();
                        var socketToSend = SocketPool[SocketId];

                        var x = await SocketWriteSmart(socketToSend, data);

                        // Insertion de l'Id du partenaire dans la base de données.
                        var current_time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                        var mysql_insert_selection = "INSERT INTO selection (order_id, partner_id,socket_id,client_id,socket_client_id,time_stamp) VALUES ('" + order_id + "','" + value["Id"] + "','" + value["socket_id"] + "','" + id_cli + "','" + "C" + id_cli + "','" + current_time + "'); ";

                        var result_insert_selection = mySqlRequest.ExecuteNonQuery(mysql_insert_selection);
                        if (result_insert_selection == false) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + mysql_insert_selection); }

                    }
                }
                else
                {
                    // pour savoir le nombre de partners a soliciter
                    var queryNbItem = 5 - result_vehicule.Count;

                    var sql_vehicule_pending = "";
                    sql_vehicule_pending += "SELECT partners.Id, type, expiration_date, capacity, latitude, longitude, score, partners.socket_id as socket_id,selection.order_id, brand as brand_name FROM partners ";
                    sql_vehicule_pending += "LEFT JOIN selection ON(partners.Id = selection.partner_id) ";
                    sql_vehicule_pending += "WHERE type=" + cat + " AND online = 1 AND available = 1 AND latitude BETWEEN " + latitudeMin.ToString().Replace(",", ".") + " AND " + latitudeMax.ToString().Replace(",", ".") + " AND order_id IS NOT NULL ";
                    //  sql_vehicule_pending += "AND longitude BETWEEN " + longitudeMin.ToString().Replace(",", ".") + " AND " + longitudeMax.ToString().Replace(",", ".") + " AND expiration_date > CURRENT_TIMESTAMP LIMIT " + queryNbItem.ToString() + "; ";

                    sql_vehicule_pending += "AND longitude BETWEEN " + longitudeMin.ToString().Replace(",", ".") + " AND " + longitudeMax.ToString().Replace(",", ".") + queryNbItem.ToString() + "; ";

                    var result_vehicule_pending = mySqlRequest.Select(sql_vehicule_pending);

                    if (result_vehicule_pending == null) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + sql_vehicule); }

                    Program.MainForm.WriteTextOnConsole(result_vehicule_pending.Count.ToString() + " Vehicule Séléctioner Pending");

                    var partnerSelected = result_vehicule;
                    //partnerSelected.AddRange(result_vehicule_pending);
                    /*
                    TODO : bug causé par un pb de doublon dans la selection                    
                    */

                    var partenerToShow = partnerSelected;

                    // Ajout des partneraire qui ne son pas en Pending dans la selection
                    foreach (var val in result_vehicule_pending)
                    {
                        bool doublon = false;
                        foreach (var valDeep in partenerToShow)
                        {
                            if (valDeep.Equals(val))
                            {
                                doublon = true;
                            }
                        }

                        if (doublon == false)
                        {
                            partnerSelected.Add(val);
                        }

                    }

                    Program.MainForm.WriteTextOnConsole(partnerSelected.Count.ToString() + " Vehicule Séléctioner au Total");

                    foreach (var value in partnerSelected)
                    {
                        Program.MainForm.WriteTextOnConsole("Id du partners-- : " + value["Id"]);
                        var data_orderVal = new
                        {
                            addr = up_addr,
                            lat_cli = lat,
                            lon_cli = lon,
                            order = order_id,
                            des_addr = down_addr,
                            des_lat = deslat,
                            des_lon = deslon,
                            des_price = price,
                            id_client = id_cli,
                            socket_client = socket_client_id,// TODO : voir si il faut mofifier ce paramettre
                            part_name = partner_name,
                            cli_name = client_name,
                            order_ts = order_tsVal,
                            taken_ts = taken_tsVal

                            /*
                            addr = jsonVal["upaddr"],
                            lat_cli = jsonVal["lat"],
                            lon_cli = jsonVal["lon"],
                            order = order_id,
                            des_addr = jsonVal["desaddr"],
                            des_lat = jsonVal["deslat"],
                            des_lon = jsonVal["deslon"],
                            des_price = jsonVal["price"],
                            id_client = jsonVal["idclient"],
                            socket_client = "0",// TODO : voir si il faut mofifier ce paramettre
                            part_name = partner_name,
                            cli_name = client_name,
                            order_ts = order_tsVal,
                            taken_ts = taken_tsVal*/
                        };

                        var data = new { action = "order", data_order = data_orderVal };

                        // Envoi de la commande au partenaire.
                        var SocketId = "P" + value["Id"].ToString();
                        var socketToSend = SocketPool[SocketId];

                        var x = await SocketWriteSmart(socketToSend, data);

                        // Insertion de l'Id du partenaire dans la base de données.
                        var current_time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                        var mysql_insert_selection = "INSERT INTO selection (order_id, partner_id,socket_id,client_id,socket_client_id,time_stamp) VALUES ('" + order_id + "','" + value["Id"] + "','" + value["socket_id"] + "','" + id_cli + "','" + "C" + id_cli + "','" + current_time + "'); ";

                        var result_insert_selection = mySqlRequest.ExecuteNonQuery(mysql_insert_selection);
                        if (result_insert_selection == false) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + mysql_insert_selection); }

                    }
                }
            }
            else
            {
                // Si aucun prestataire ne repond alors on solicite que des partenaire en pending

                var sql_vehicule_pending = "";
                sql_vehicule_pending += "SELECT partners.Id, type, expiration_date, capacity, latitude, longitude, score, partners.socket_id as socket_id,selection.order_id, brand as brand_name FROM partners ";
                sql_vehicule_pending += "LEFT JOIN selection ON(partners.Id = selection.partner_id) ";
                sql_vehicule_pending += "WHERE type=" + cat + " AND online = 1 AND available = 1 AND latitude BETWEEN " + latitudeMin.ToString().Replace(",", ".") + " AND " + latitudeMax.ToString().Replace(",", ".") + " AND order_id IS NOT NULL ";
                // sql_vehicule_pending += "AND longitude BETWEEN " + longitudeMin.ToString().Replace(",", ".") + " AND " + longitudeMax.ToString().Replace(",", ".") + " AND expiration_date > CURRENT_TIMESTAMP LIMIT 5; ";

                sql_vehicule_pending += "AND longitude BETWEEN " + longitudeMin.ToString().Replace(",", ".") + " AND " + longitudeMax.ToString().Replace(",", ".") + "LIMIT 5; ";



                var result_vehicule_pending = mySqlRequest.Select(sql_vehicule_pending);

                if (result_vehicule_pending == null) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + sql_vehicule); }

                if (result_vehicule_pending.Count > 0)
                {
                    Program.MainForm.WriteTextOnConsole(result_vehicule_pending.Count.ToString() + " Vehicule Séléctioner");

                    var partnerSelected = result_vehicule_pending;
                    //partnerSelected.AddRange(result_vehicule_pending);

                    foreach (var value in partnerSelected)
                    {
                        Program.MainForm.WriteTextOnConsole("Id du partners : " + value["Id"]);
                        var data_orderVal = new
                        {
                            addr = up_addr,
                            lat_cli = lat,
                            lon_cli = lon,
                            order = order_id,
                            des_addr = down_addr,
                            des_lat = deslat,
                            des_lon = deslon,
                            des_price = price,
                            id_client = id_cli,
                            socket_client = socket_client_id,// TODO : voir si il faut mofifier ce paramettre
                            part_name = partner_name,
                            cli_name = client_name,
                            order_ts = order_tsVal,
                            taken_ts = taken_tsVal

                            /*
                            addr = jsonVal["upaddr"],
                            lat_cli = jsonVal["lat"],
                            lon_cli = jsonVal["lon"],
                            order = order_id,
                            des_addr = jsonVal["desaddr"],
                            des_lat = jsonVal["deslat"],
                            des_lon = jsonVal["deslon"],
                            des_price = jsonVal["price"],
                            id_client = jsonVal["idclient"],
                            socket_client = "0",// TODO : voir si il faut mofifier ce paramettre
                            part_name = partner_name,
                            cli_name = client_name,
                            order_ts = order_tsVal,
                            taken_ts = taken_tsVal
                            */
                        };

                        var data = new { action = "order", data_order = data_orderVal };

                        // Envoi de la commande au partenaire.
                        var SocketId = "P" + value["Id"].ToString();
                        var socketToSend = SocketPool[SocketId];

                        var x = await SocketWriteSmart(socketToSend, data);

                        // Insertion de l'Id du partenaire dans la base de données.
                        var current_time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                        var mysql_insert_selection = "INSERT INTO selection (order_id, partner_id,socket_id,client_id,socket_client_id,time_stamp) VALUES ('" + order_id + "','" + value["Id"] + "','" + value["socket_id"] + "','" + id_cli + "','" + "C" + id_cli + "','" + current_time + "'); ";

                        var result_insert_selection = mySqlRequest.ExecuteNonQuery(mysql_insert_selection);
                        if (result_insert_selection == false) { Program.MainForm.WriteTextOnConsole("Mysql Error: " + mysql_insert_selection); }
                    }

                }
                else
                {
                    Program.MainForm.WriteTextOnConsole("Aucun Vehicule Séléctioner");
                    // Si aucun prestataire ne repond on supprime la commande
                    var sql_delete_last_order = "DELETE FROM confirmation WHERE id_client = '" + id_cli.ToString() + "' and id_vehicule IS NULL;";
                    if (mySqlRequest.ExecuteNonQuery(sql_delete_last_order) == false) { Program.MainForm.WriteTextOnConsole("DELET LAST ORDER Mysql Error :" + sql_delete_last_order); }

                    if (cancel)
                    {
                        // On recupere la raison dans le database                        

                        var selectInfoCancel = "SELECT * FROM refus_course WHERE id_order = '" + order_id + "';";
                        var resultInfoCancel = mySqlRequest.Select(selectInfoCancel);
                        if (resultInfoCancel == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + selectInfoCancel); }

                        var motifCancel = resultInfoCancel[0]["motif_refus"].ToString();
                        var text_Mess = resultInfoCancel[0]["text_mess"].ToString();

                        // On determine le bon message en fonction de la raison de l'anulation de commande du partenaire
                        var message = "";

                        switch (motifCancel)
                        {
                            case "1":// Accident
                                message = "Desole,Notre parteanire a eu un accident sur le chemin. Merci de bien vouloir refiare une demande.";
                                break;

                            case "2":// probleme mecanique
                                message = "Desole,Notre partenaire a eu un probleme mecanique. Merci de bien vouloir refiare une demande.";
                                break;

                            case "3"://autre
                                message = "Desole,Nous avons pas trouver de partner pour votre voyage.";
                                break;
                        }
                        var infoCancel = new { motif = motifCancel, text_mess = text_Mess };
                        var data = new { action = "cancel", data = infoCancel };
                        var json_to_send = data;
                        var x = await SocketWriteSmart(socketCliFinale, json_to_send);
                        // Envoyert une remote notification pour infiormé le client que nous avons pas de partenaires disponnible.

                        var socketId = "C" + id_cli.ToString();
                        var messageToSend = message;
                        //await Task.Run(() => { SendClientRemoteNotification(socketId, messageToSend); });
                        //await Task.Run(() => { SendClientRemoteNotification("C32", "bob"); });

                    }

                    else
                    {
                        // TODO : Corriger cette procedure il faut un autre message car la procedure timeout n est pas faite pour ca
                        // On evoie au client final un timeout pour qu il puisse refaire une selection ou le liberer
                        // NEED DEBUG
                        var data = new { action = "time_out" };
                        var json_to_send = data;
                        var x = await SocketWriteSmart(socket, json_to_send);
                    }
                }
            }

        }

        /// <summary>
        /// Action for the "Accept" keyword
        /// </summary>
        /// <param name="jsonVal"></param>
        /// <param name="clientId"></param>
        private async void ActionForAccept(JObject jsonVal, string clientId)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();
            var partnerId = clientId.Replace("P", string.Empty);

            var sql = "SELECT * FROM selection WHERE order_id = '" + jsonVal["order"] + "'";

            var result = mySqlRequest.Select(sql);

            if (result == null) { Program.MainForm.WriteTextOnConsole("Mysql Error :" + sql); }

            // Envoie de la commande time_out au partenaire pour qu il puisse supprimer cette commande de la pile de commande.
            var orderId = jsonVal["order"];
            foreach (var val in result)
            {
                // TODO : Vois si il faut envoyer une reponse au partenaire pour valider le numero de command pour l ajouter a la pile

                if (val["partner_id"] != partnerId)
                {
                    var data = new { action = "time_out", order = orderId };
                    var socket = SocketPool["P" + val["partner_id"]];
                    var x = await SocketWriteSmart(socket, data);
                    Program.MainForm.WriteTextOnConsole("@P" + val["partner_id"] + "-->" + data.ToString());
                }
            }

            // on supprime dans selection tous les entrés avec order_id
            var sql_delete_order_id = "DELETE FROM selection WHERE order_id = '" + jsonVal["order"] + "'";

            var resultDelete_order_id = mySqlRequest.ExecuteNonQuery(sql_delete_order_id);
            if (resultDelete_order_id == false) { Program.MainForm.WriteTextOnConsole("Mysql Error : " + sql_delete_order_id); }

            // on supprime toutes les entrés avec partner_id
            var sql_delete_partner_id = "DELETE FROM selection WHERE partner_id = '" + partnerId + "'";

            var resultDelete_partner_id = mySqlRequest.ExecuteNonQuery(sql_delete_partner_id);
            if (resultDelete_partner_id == false) { Program.MainForm.WriteTextOnConsole("Mysql Error : " + sql_delete_partner_id); }

            // Mise a jour du numero de partenaire dans la table confirmation (qui represente la liste des commande)
            var sql_update_order = "UPDATE confirmation SET id_vehicule=" + partnerId + ",status=1 WHERE  Id=" + jsonVal["order"] + ";";
            var result_update_order = mySqlRequest.ExecuteNonQuery(sql_update_order);
            if (result_update_order == false) { Program.MainForm.WriteTextOnConsole("Mysql Error sql_update_order"); }


            // Recuperation des informations de la commande dans la database.
            var sql_order = "SELECT * FROM confirmation co INNER JOIN client cl ON cl.Id_client = co.id_client WHERE co.Id = '" + jsonVal["order"] + "'";
            var result_order = mySqlRequest.Select(sql_order);
            if (result_order == null) { Program.MainForm.WriteTextOnConsole("Mysql Error : " + sql_order); }

            var value = result_order[0];

            var sql_vehicule = "SELECT * FROM partners WHERE partners.id = " + partnerId + ";";

            var result_vehicule = mySqlRequest.Select(sql_vehicule);
            if (result_vehicule == null) { Program.MainForm.WriteTextOnConsole("Mysql Error : sql_vehicule"); }

            var partners = result_vehicule[0];

            sql_update_order = string.Empty;

            sql_update_order += "UPDATE confirmation SET ";

            sql_update_order += "part_capacity = '" + partners["capacity"] + "',";

            sql_update_order += "part_brand = '" + partners["brand"] + "',";

            sql_update_order += "part_first_name = '" + partners["first_name"] + "',";

            sql_update_order += "part_last_name = '" + partners["last_name"] + "',";

            sql_update_order += "part_email = '" + partners["email"] + "',";

            sql_update_order += "part_rib = '" + partners["rib"] + "',";

            sql_update_order += "part_phone = '" + partners["phone"] + "',";

            sql_update_order += "part_password = '" + partners["password"] + "',";

            sql_update_order += "part_socket_id = '" + partners["socket_id"] + "'";

            sql_update_order += "WHERE Id = " + jsonVal["order"] + "; ";

            result_update_order = mySqlRequest.ExecuteNonQuery(sql_update_order);
            if (result_update_order == false) { Program.MainForm.WriteTextOnConsole("mysql Error sql_update_order : " + sql_update_order); }

            var transationstatus = "1";
            var service_json = "";
            // TODO : voir si il faut pas virer les services

            double distanceVal = 0;

            if (transationstatus == "2" || transationstatus == "1")
            {
                string latCli = value["lat_cli"];
                latCli = latCli.Replace(".", ",");

                var lonCli = value["lon_cli"];
                lonCli = lonCli.Replace(".", ",");

                var latCliDbl = double.Parse(latCli);
                var lonCliDbl = double.Parse(lonCli);

                var latitudeDbl = double.Parse(partners["latitude"]);
                var longitudeDbl = double.Parse(partners["longitude"]);

                distanceVal = Math.Round(GetDistance(latCliDbl, lonCliDbl, latitudeDbl, longitudeDbl) / 1000, 3);

            }
            // Envoie des informations du partenaire au client finale.
            var dataPart = new
            {
                Id = partners["Id"],
                type = partners["type"],
                capacity = partners["capacity"],
                latitude = partners["latitude"],
                longitude = partners["longitude"],
                brand = partners["brand"],
                score = partners["score"],
                phone = partners["phone"],
                first = partners["first_name"],
                last = partners["last_name"],
                orderid = orderId,
                service = service_json, // pour les caractere accentué du json
                transationstatus = transationstatus.ToString(),
                distance = distanceVal
            };

            var dataToSend = new { action = "partner_info", data = dataPart };
            //TODO : Verifier ce parametre (voir si ilfaut pas reconstituer l'id du socket

            string socketId = "C" + value["id_client"].ToString();
            bool needToSendPush = false;

            if (SocketPool.FirstOrDefault(x => x.Key == socketId).Value != null)
            {
                // Envoi de l information au client final par le socket
                // en cas d echec on envoi un message push

                var SocketClientFinal = SocketPool[socketId];
                var transmission = await SocketWriteSmart(SocketClientFinal, dataToSend);


                if (transmission.Result == false)
                {
                    needToSendPush = true;
                }

            }
            else
            {
                needToSendPush = true;
            }

            if (needToSendPush)
            {
                /*
                string filepath = ApnCerFinalFilepath;
                string pwd = ApnCerFinalPwd;
                PushNotification pushNotification = new PushNotification(PushNotificationType.Development, filepath, pwd);
                PushNotificationPayload payload = new PushNotificationPayload();
                payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == socketId).Value;
                Console.WriteLine("push token : "+payload.deviceToken);
                payload.badge = 0;
                payload.sound = "default";
                payload.message = message;
                pushNotification.Push(payload);
                */

                /*
                var t = Task.Run(() => {
                    string filepath = ApnCerFinalFilepath;
                    string pwd = ApnCerFinalPwd;
                    PushNotification pushNotification = new PushNotification(PushNotificationType.Development, filepath, pwd);
                    PushNotificationPayload payload = new PushNotificationPayload();
                    payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == socketId).Value;
                    Console.WriteLine("push token : " + payload.deviceToken);
                    payload.badge = 0;
                    payload.sound = "default";
                    payload.message = message;
                    pushNotification.Push(payload);
                });
                */

                //var test = Task.Run(() => { Program.TcpServer.testMsg(); });
                var message = "Votre vehicule est en route.";
                var t = Task.Run(() => SendClientRemoteNotification(socketId, message));

            }

        }



        /// <summary>
        /// Action for the "Order refuse"
        /// </summary>
        /// <param name="jsonVal"></param>
        /// <param name="partnerId"></param>
        private async void ActionForOrderRefuse(JObject jsonVal, string partnerId)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();
            // TODO : Supprimer l entree avec le numero de socket de l envoie
            // TODO : faire un select de toute les selections avec le numero de commande communiquer et si le compte est = 0 envoyer un message au client
            var sql_delete_selection = "DELETE FROM selection WHERE order_id = '" + jsonVal["order_id"] + "' AND partner_id = '" + partnerId + "';";
            var result_delete_selection = mySqlRequest.ExecuteNonQuery(sql_delete_selection);

            if (result_delete_selection == false) { Program.MainForm.WriteTextOnConsole("Mysql_Error : " + sql_delete_selection); }


            // On compte le nombre d'enregistrement dans selection avec le numero de commande concerné. Si il est a 0 alors on contact le cleint par tcp
            var sql_active_selection_num_rows = "SELECT * FROM selection WHERE order_id = '" + jsonVal["order_id"] + "'";

            var result_active_selection_num_rows = mySqlRequest.Select(sql_active_selection_num_rows);

            if (result_active_selection_num_rows == null) { Program.MainForm.WriteTextOnConsole("Mysql Error : " + result_active_selection_num_rows); }

            if (result_active_selection_num_rows.Count == 0)
            {
                // Si tous les partenaire contacter refuse la course alors on envoie un message au client.
                // Et on supprime la commande.
                var sql_find_socket_client = "SELECT socket_id FROM confirmation co INNER JOIN client cl ON co.id_client = cl.Id_client WHERE co.Id = '" + jsonVal["order_id"] + "' ";

                var result_find_socket_client = mySqlRequest.Select(sql_find_socket_client);

                if (result_find_socket_client == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + sql_find_socket_client); }

                var val_socket = result_find_socket_client[0];

                var sql_delete_last_order = "DELETE FROM confirmation WHERE Id = '" + jsonVal["order_id"] + "';";

                if (mySqlRequest.ExecuteNonQuery(sql_delete_last_order) == false) { Program.MainForm.WriteTextOnConsole("DELET LAST ORDER Mysql Error"); }

                // Envoie d'une action sur le mocile client finale

                var data = new object();

                // En cas d'annulation on envoie cancel comme message.
                // sinon on envoie le message par defaut
                var sql_verify_cancel_order = "SELECT id FROM refus_course WHERE id ='" + jsonVal["order_id"] + "'";
                var result_verify_cancel_order = mySqlRequest.Select(sql_verify_cancel_order);
                if (result_verify_cancel_order.Count == 0)
                {
                    data = new { action = "time_out" };
                }
                else
                {
                    data = new { action = "cancel" };
                }

                var json_to_send = data;

                var socketClientFinal = SocketPool[val_socket["socket_id"]];

                var x = await SocketWriteSmart(socketClientFinal, json_to_send);

            }

        }

        /// <summary>
        /// action for the "clientAppApprochaed" keyword
        /// </summary>
        /// <param name="jsonVal"></param>
        async void ActionForClientApproched(JObject jsonVal)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();
            // Mise a jour de l etat de la commande au cas ou le partenaire aurai une perte de connection.
            var sql_update_order = "UPDATE confirmation SET status=2,pickup_arrived_ts=CURRENT_TIMESTAMP WHERE  Id=" + jsonVal["order"] + ";";
            if (mySqlRequest.ExecuteNonQuery(sql_update_order) == false) { Program.MainForm.WriteTextOnConsole("Mysql Error sql_update_order"); }

            //TODO : si le socket client n existe pas verifier l'id du client finale
            var data = new { action = "client_approached" };
            var idSocketClientFinal = "C" + jsonVal["id_client"].ToString();
            bool needToPush = false;
            //var message = "Votre vehicule est arrivé.";

            if (SocketPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value != null)
            {
                // Envoi de l information au client final par le socket
                // en cas d echec on envoi un message push

                var socketClientFinal = SocketPool[idSocketClientFinal];
                var transmission = await SocketWriteSmart(socketClientFinal, data);

                if (transmission.Result == false)
                {
                    needToPush = true;
                }
            }
            else
            {
                needToPush = true;
            }

            if (needToPush)
            {
                /*
                string filepath = ApnCerFinalFilepath;
                string pwd = ApnCerFinalPwd;
                PushNotification pushNotification = new PushNotification(PushNotificationType.Development, filepath, pwd);
                PushNotificationPayload payload = new PushNotificationPayload();
                payload.deviceToken = TokenPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value;
                payload.badge = 1;
                payload.sound = "default";
                payload.message = message;
                pushNotification.Push(payload);
                */
                //var idToken = TokenPool.FirstOrDefault(x => x.Key == idSocketClientFinal).Value;

                //testMsg();

                //var message = "Votre vehicule est arrive.";
                //var t = Task.Run(()=>SendClientRemoteNotification(idSocketClientFinal, message));

            }

            Program.MainForm.WriteTextOnConsole("Envoie de message push");

            var message = "Votre vehicule est a moins de 500 metres de votre position.";
            var t = Task.Run(() => SendClientRemoteNotification(idSocketClientFinal, message));

        }


        /// <summary>
        /// 
        /// </summary>
        async void ActionForNoCustomer(JObject jsonVal)
        {
            // Archivage de la commande et operation stripe pour annulation(modification du transfere et du payment Intent)
            var orderId = jsonVal["order_id"].ToString();
            var clientId = jsonVal["id_client"].ToString();
            StripeOperationCancelOrder(clientId);

            // Envoyer une commande TCP a la device du client final par le biais du SocketTCP.

            var idSocketClient = "C" + jsonVal["id_client"].ToString();

            var data = new { action = "customer_not_find" };


            // On verifie si la device est en ligne.
            if (SocketPool.FirstOrDefault(x => x.Key == idSocketClient).Value != null)
            {
                var client_final = SocketPool[idSocketClient];
                var x = await SocketWriteSmart(client_final, data);
            }

            var msg = "Votre chauffeur ne vous a pas trouvé à l'emplacement que vous avez specifié sur la commande et cela depuis plus de 4 minutes. Vous serez facturés de 5 €.";
            var t = Task.Run(() => { SendClientRemoteNotification(idSocketClient, msg); });

            // On archive la commande
            ArchiveOrder(orderId.ToString(), "6");
        }

        /// <summary>
        /// Action for clientTaken  keyword
        /// </summary>
        /// <param name="jsonVal"></param>
        /// <param name="socketPartner"></param>
        async void ActionForClientTaken(JObject jsonVal, ITcpSocketClient socketPartner)
        {
            MySqlRequest mySqlRequest = new MySqlRequest();

            // Mise a jour de l etat de la commande au cas ou le partenaire aurai une perte de connection.
            var sql_update_order = "UPDATE confirmation SET status=3,taken_ts=CURRENT_TIMESTAMP WHERE  Id=" + jsonVal["order"] + ";";
            if (mySqlRequest.ExecuteNonQuery(sql_update_order) == false) { Program.MainForm.WriteTextOnConsole("Mysql Error sql_update_order : " + sql_update_order); }

            //TODO : si le socket client n existe pas verifier l'id du client finale
            var data = new { action = "client_taken", id = jsonVal["order"] };

            //bool needToPush = true;
            bool needToPush = false; //DEBUG
            var idSocketClientFinal = "C" + jsonVal["id_client"].ToString();

            if (SocketPool.ContainsKey(idSocketClientFinal))
            {
                var socketClientFinal = SocketPool["C" + jsonVal["id_client"]];

                var transmission = await SocketWriteSmart(socketClientFinal, data);
                if (transmission.Result == false)
                {
                    needToPush = true;
                }
            }
            else
            {
                needToPush = true;
            }

            //Envoie des informations au partner pour le billet collectif
            /*var sql_select_confirmation = "SELECT * FROM confirmation WHERE confirmation.Id = '" + jsonVal["order"] + "' ";*/

            var sql_select_confirmation = "SELECT confirmation.Id,taken_ts,prise_en_charge,prix_au_km,prix_par_min" +
                " FROM confirmation LEFT JOIN partners ON(partners.Id = confirmation.id_vehicule)" +
                " LEFT JOIN tarification ON(tarification.id = partners.`type`)  WHERE confirmation.Id = '" + jsonVal["order"] + "'";

            var result_select_confirmation = mySqlRequest.Select(sql_select_confirmation);
            if (result_select_confirmation == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + sql_select_confirmation); }

            var val_confirmation = result_select_confirmation[0];
            var data_partner = new
            {
                action = "client_taken",
                id_order = val_confirmation["Id"].ToString(),
                taken_ts = val_confirmation["taken_ts"],
                base_price = val_confirmation["prise_en_charge"],
                price_km = val_confirmation["prix_au_km"],
                price_min = val_confirmation["prix_par_min"]
            };

            if (SocketPool.ContainsValue(socketPartner))
            {
                var transmission = await SocketWriteSmart(socketPartner, data_partner);
                if (transmission.Result == false)
                {
                    Console.WriteLine("Erreur envoie d info de billet collectif");
                }
            }

            //TODO : BugFix(ne marche pas need debug)
            if (needToPush)
            {
                var message = "Etes vous bien installer.";
                var t = Task.Run(() => SendClientRemoteNotification(idSocketClientFinal, message));

            }

        }

        /// <summary>
        /// L'action "release_client" est envoyer au client finale pour liberer son application et qu'il puisse passé une autre commande.
        /// </summary>
        /// <param name="jsonVal"></param>
        async void ActionForReleaseClient(JObject jsonVal, string partnerId)
        {
            //======================= CAPTURE DU PAIEMENT ==============================

            MySqlRequest mySqlRequest = new MySqlRequest();

            // On effectue la capture du payment
            var sql_select_pi_confirmation = "SELECT pspReference,confirmation.Id,partners.Id As partnerId, stripe_account, driver_net_price,com_stripe,com_ct8, partners.abo_week_paid,tarification.abonnement FROM confirmation LEFT JOIN partners ON confirmation.id_vehicule = partners.Id LEFT JOIN tarification ON partners.`type` = tarification.id WHERE confirmation.Id = '" + jsonVal["order"] + "';";


            var result_select_pi_confirmation = mySqlRequest.Select(sql_select_pi_confirmation);
            if (result_select_pi_confirmation == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + sql_select_pi_confirmation); }

            var val_pi_confirmation = result_select_pi_confirmation[0];
            var paymentIntentId = val_pi_confirmation["pspReference"].ToString();
            var partnerStripeAccount = val_pi_confirmation["stripe_account"].ToString();
            var partnerAboPrice = val_pi_confirmation["abonnement"].ToString();
            var partnerAllReadyPaid = val_pi_confirmation["abo_week_paid"].ToString();
            //var partnerNetPrice = val_pi_confirmation["driver_net_price"].ToString();
            //var comPartner = val_pi_confirmation["driver_net_price"].ToString();
            //var comCt8 = val_pi_confirmation["com_ct8"].ToString();

            CultureInfo cultureInfo = new CultureInfo("fr-FR"); //Nécéssaire pour le conversion de string en float ou double.

            var idOrder = val_pi_confirmation["Id"].ToString();

            var requestOption = new RequestOptions();
           // requestOption.StripeConnectAccountId = partnerStripeAccount;
            requestOption.StripeAccount = partnerStripeAccount;


            StripeConfiguration.ApiKey = "sk_test_dBR2X5yRuQklppKxRz7jCbuT";

            var paymentIntentService = new PaymentIntentService();

            var paymentIntentOptions = new PaymentIntentUpdateOptions()
            {
                Description = "Order : " + idOrder,
            };

            var x = await paymentIntentService.UpdateAsync(paymentIntentId, paymentIntentOptions);

            var pi = paymentIntentService.Capture(paymentIntentId, null);

            // Une fois que le paiement est capturé on recupère les montants des frais stripe, le net et le brut            
            var chargeId = pi.Charges.Data[0].Id;
            var balanceTransactionId = pi.Charges.Data[0].BalanceTransactionId;

            var serviceBalTxn = new BalanceTransactionService();
            var balanceTransaction = serviceBalTxn.Get(balanceTransactionId);

            var RealcomStripe = balanceTransaction.Fee;     // en centime
            var realBrutAmount = balanceTransaction.Amount; // idem
            var realNetAmount = balanceTransaction.Net;     // idem 
            var RealCt8Com = Math.Round(((realNetAmount * 0.014) + 73), 2); //  idem 
            RealCt8Com = (int)RealCt8Com;

            // Calcule du montant

            double partnerAboPriceDouble = double.Parse(partnerAboPrice);
            double partnerAllReadyPaidDouble = double.Parse(partnerAllReadyPaid);
            double partnerNetPriceDouble = (double)realNetAmount - RealCt8Com;
            double transferAmount = (partnerAllReadyPaidDouble + partnerNetPriceDouble) - partnerAboPriceDouble;

            // si transferamount est inferieure a 0 alors le partner doit encore payé une partie de l abo
            // du coup on ne lui transfer rien
            transferAmount = (transferAmount <= 0) ? 0 : transferAmount;

            string stripeTransferId = "";
            string stripeBalanceTransaction = "";
            string stripeDestinationAccount = "";
            string stripeDestinationPaiement = "";
            string stripeSourceTransaction = "";
            string stripeTransferGroup = "";
            List<string> paramForArchive = new List<string>();



            // sinon on transfere la difference
            if (transferAmount > 0)
            {
                double dFinalAmount = 100 * transferAmount;
                long finalAmount = (long)dFinalAmount;

                // On lance l'ordre de transfere chez stripe.
                var metadata = new Dictionary<string, string>();

                metadata.Add("OrderId", idOrder);
                metadata.Add("ookcity", (RealCt8Com / 100).ToString());
                metadata.Add("partner", (((double)realNetAmount - RealCt8Com) / 100).ToString());
                metadata.Add("stripe", ((double)RealcomStripe / 100).ToString());

                var options = new TransferCreateOptions
                {
                    Amount = (long)(realNetAmount - RealCt8Com),
                    Currency = "eur",
                    Destination = partnerStripeAccount,
                    SourceTransaction = chargeId,
                    Metadata = metadata
                };

                var service = new TransferService();
                Transfer transfer = service.Create(options);
                stripeTransferId = transfer.Id;
                stripeBalanceTransaction = transfer.BalanceTransactionId;
                stripeDestinationAccount = transfer.DestinationId;
                stripeDestinationPaiement = transfer.DestinationPaymentId;
                stripeSourceTransaction = transfer.SourceTransactionId;
                stripeTransferGroup = transfer.TransferGroup;
                // Ajout des datas de commission dans une variable tableau pour archivage

                var com_stripe = ((double)RealcomStripe / 100).ToString();
                var com_ct8 = (RealCt8Com / 100).ToString();
                var driver_net_price = (((double)realNetAmount - RealCt8Com) / 100).ToString();

                paramForArchive.Add(stripeTransferId);
                paramForArchive.Add(stripeBalanceTransaction);
                paramForArchive.Add(stripeDestinationAccount);
                paramForArchive.Add(stripeDestinationPaiement);
                paramForArchive.Add(stripeSourceTransaction);
                paramForArchive.Add(stripeTransferGroup);

                paramForArchive.Add(com_stripe);
                paramForArchive.Add(com_ct8);
                paramForArchive.Add(driver_net_price);


            }

            double newAllReadyPaid = partnerAllReadyPaidDouble + partnerNetPriceDouble - transferAmount;

            // Update de du montant payé dans la base de donnée
            var updatePartnerAboPaid = "UPDATE partners SET abo_week_paid = " + newAllReadyPaid.ToString().Replace(",", ".") + " WHERE Id=" + partnerId + ";";
            mySqlRequest.ExecuteNonQuery(updatePartnerAboPaid);

            //==========================  EOF CAPTURE DU PAIEMENT ===============================================

            //var clientId = ArchiveOrder(jsonVal["order"].ToString());
            var clientId = ArchiveOrder(jsonVal, paramForArchive);

            // Envoie de la commande tcp pour liberer le client
            // Transmission de l'Order id pour le que client puisse donné une note a sa course;
            var orderId = jsonVal["order"];

            var data = new { action = "release_client", order = orderId };

            var idSocketClient = "C" + clientId;

            if (SocketPool.ContainsKey(idSocketClient))
            {
                var client_final = SocketPool["C" + clientId];
                var y = await SocketWriteSmart(client_final, data);
            }

            //TODO : voir si il faut pas envoyer un remote message quand le client arrive.

        }


        /// <summary>
        /// Vide le flux du socket et envoie les datas vers le endpoint.
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="obj"></param>
        private async Task<ClientWriteSmartResult> SocketWriteSmart(ITcpSocketClient socket, object obj)
        {
            JObject dataToSend = JObject.FromObject(obj);
            //dataToSend["action"] = "connection_result";
            //dataToSend["status"] = 3;

            var serialized = JsonConvert.SerializeObject(dataToSend);
            string input = serialized;
            // Code pour affiché les logs
            // Affiche les logs de toutes les data qui transit.
            var culture = new CultureInfo("fr-FR");
            DateTime localDate = DateTime.Now;
            //DateTime utcDate = DateTime.UtcNow; // Decomenter pour avoir une date UTC

            var socketId = SocketPool.FirstOrDefault(x => x.Value == socket).Key;

            //    Program.MainForm.WriteTextOnConsole(localDate.ToString(culture) + " : @" + socketId + " ---> " + input.ToString());
            //    Console.WriteLine(localDate.ToString(culture) + " : " + input);


            byte[] buffer = ASCIIEncoding.UTF8.GetBytes(input);

            ClientWriteSmartResult clientWriteSmartResult = new ClientWriteSmartResult();

            try
            {
                socket.WriteStream.Write(buffer, 0, buffer.Count());
                await socket.WriteStream.FlushAsync();

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                clientWriteSmartResult.Result = false;
                clientWriteSmartResult.ExceptionDetails = e;
            }

            if (clientWriteSmartResult.ExceptionDetails == null)
            {
                clientWriteSmartResult.Result = true;
            }

            return clientWriteSmartResult;
        }

        /// <summary>
        /// Demarre
        /// </summary>
        public async void Start()
        {

            // bind to the listen port across all interfaces
            await listener.StartListeningAsync(ListenPort);
            Program.MainForm.WriteTextOnConsole("STARTED V" + this.Version);
            Program.MainForm.ChangeStateOfStartButton(1);
            this.serverIsStarted = true;
            this.cleanUpTable = true;
            Task t = new Task(CleanUpSelectionTable);
            t.Start();


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <returns></returns>
        private List<string> StripeOperationCancelOrder(string clientId)
        {
            // Recuperation de toutes les information necassaire dans la base de donnée
            MySqlRequest mySqlRequest = new MySqlRequest();
            var sql_select_pi_confirmation = "SELECT confirmation.Id AS orderId,abo_week_paid,partners.Id AS partnerId,pspReference,type,stripe_account FROM confirmation LEFT JOIN partners ON partners.Id = confirmation.id_vehicule  WHERE confirmation.id_client = '" + clientId + "';";
            var result_select_pi_confirmation = mySqlRequest.Select(sql_select_pi_confirmation);
            if (result_select_pi_confirmation == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + sql_select_pi_confirmation); }

            var partnerId = result_select_pi_confirmation[0]["partnerId"];
            var partnerAllReadyPaid = result_select_pi_confirmation[0]["abo_week_paid"];
            var orderId = result_select_pi_confirmation[0]["orderId"];

            var val_pi_confirmation = result_select_pi_confirmation[0];
            var paymentIntentId = val_pi_confirmation["pspReference"].ToString();
            var typeDeVehicule = val_pi_confirmation["type"].ToString();
            var partnerStripeAccount = val_pi_confirmation["stripe_account"].ToString();

            // Récuperation des information de tarification
            var sql_select_tarificationn = "SELECT * FROM tarification WHERE id ='" + typeDeVehicule + "'";
            var result_tarification = mySqlRequest.Select(sql_select_tarificationn);
            if (result_tarification == null) { Program.MainForm.WriteTextOnConsole("mySqlRequest Error : " + sql_select_tarificationn); }
            var val_tarification = result_tarification[0];

            // Recuperation des Tarif d'annulation dans la database
            var partnerAboPrice = val_tarification["abonnement"];
            var partnerNetPriceStr = val_tarification["cancel_fee"];
            var partnerNetPrice = double.Parse(partnerNetPriceStr) * 100;

            // Calcule du montant
            double partnerAboPriceDouble = double.Parse(partnerAboPrice);
            double partnerAllReadyPaidDouble = double.Parse(partnerAllReadyPaid);
            double partnerNetPriceDouble = partnerNetPrice;
            // Montant du transfer est agale a (l'abo deja paye + le montant de la commande) moins le prix totale de l'abonement
            double transferAmount = (partnerAllReadyPaidDouble + (partnerNetPriceDouble / 100)) - partnerAboPriceDouble;

            // si transferamount est inferieure a 0 alors le partner doit encore payé une partie de l abo
            // du coup on ne lui transfer rien
            transferAmount = (transferAmount <= 0) ? 0 : transferAmount;

            // Operation de payment Stripe
            StripeConfiguration.SetApiKey("sk_test_dBR2X5yRuQklppKxRz7jCbuT");
            var paymentIntentService = new PaymentIntentService();
            var piToUpdateWithMetaData = paymentIntentService.Get(paymentIntentId);
            var metadata = new Dictionary<string, string>();

            // Calcule des montant pour le tranfer stripe
            // multiplication des montants par 100 pour eviter les erreure d'arrondi.
            var basePrice = partnerNetPrice;

            // Ajout des metadata dans la description des Operation Strtipe
            metadata.Add("OrderId", orderId);
            metadata.Add("Type", "Annulation Client Finale");

            var updateOption = new PaymentIntentUpdateOptions
            {
                Metadata = metadata
            };

            // MAJ des metha-data du paiement Intent
            paymentIntentService.Update(paymentIntentId, updateOption);

            // Modification du montant du paiement itente avant de faire la capture
            int amountToCapture = (int)partnerNetPrice;
            // On Capture le payment Intent avec le nouveau prix.
            var captureOptions = new PaymentIntentCaptureOptions
            {
                AmountToCapture = amountToCapture
            };

            var pi = paymentIntentService.Capture(paymentIntentId, captureOptions);
            var chargeId = pi.Charges.Data[0].Id;

            // On recupere les balance Transaction pour le paiement de base
            var balanceTransactionId = pi.Charges.Data[0].BalanceTransactionId;
            // On recupere la balance transaction du remboursement.
            var balanceTransactionRefundId = pi.Charges.Data[0].Refunds.Data[0].BalanceTransactionId;

            var serviceBalTxn = new BalanceTransactionService();
            var balanceTransaction = serviceBalTxn.Get(balanceTransactionId);
            var balanceTransactionRefund = serviceBalTxn.Get(balanceTransactionRefundId);
            // On calcule les commissions a partir des informations des balance Transaction. 
            var RealcomStripe = balanceTransaction.Fee + balanceTransactionRefund.Fee;     // en centime
            var realBrutAmount = balanceTransaction.Amount + balanceTransactionRefund.Amount; // idem
            var realNetAmount = balanceTransaction.Net + balanceTransactionRefund.Net;     // idem 
            var RealCt8Com = Math.Round(((realNetAmount * 0.014) + 73), 2); //  idem 
            RealCt8Com = (int)RealCt8Com;

            // Calacul du transfere pour le partenaire
            long finalAmount = (long)basePrice - ((long)RealCt8Com + RealcomStripe);

            // Déclaration de variables pour stocker les informations concernant la balance Transaction.
            string stripeTransferId = "";
            string stripeBalanceTransaction = "";
            string stripeDestinationAccount = "";
            string stripeDestinationPaiement = "";
            string stripeSourceTransaction = "";
            string stripeTransferGroup = "";
            List<string> paramForArchive = new List<string>();

            // Orde de transfer au partenaire si necessaire.
            if (transferAmount > 0)
            {
                // On ajoute les metadata pour le transfere
                var metadataTr = new Dictionary<string, string>();

                metadataTr.Add("OrderId", orderId);
                metadataTr.Add("ookcity", (RealCt8Com / 100).ToString());
                metadataTr.Add("partner", (((double)realNetAmount - RealCt8Com) / 100).ToString());
                metadataTr.Add("stripe", ((double)RealcomStripe / 100).ToString());

                // On lance l'ordre de transfer chez stripe.
                //var metadata = new Dictionary<string, string>();

                var options = new TransferCreateOptions
                {
                    Amount = finalAmount,
                    Currency = "eur",
                    Destination = partnerStripeAccount,
                    SourceTransaction = chargeId,
                    Metadata = metadataTr
                };

                // Conversion des datas de comission en type String.
                var com_stripe = ((double)RealcomStripe / 100).ToString();
                var com_ct8 = (RealCt8Com / 100).ToString();
                var driver_net_price = ((double)finalAmount / 100).ToString();

                // Envoie de la requette stripe pour le Transfere
                var service = new TransferService();
                Transfer transfer = service.Create(options);

                // Recuperation des informations de transfer pour les ajouter a la database au moment de l'archivage.
                stripeTransferId = transfer.Id;
                stripeBalanceTransaction = transfer.BalanceTransactionId;
                stripeDestinationAccount = transfer.DestinationId;
                stripeDestinationPaiement = transfer.DestinationPaymentId;
                stripeSourceTransaction = transfer.SourceTransactionId;
                stripeTransferGroup = transfer.TransferGroup;

                paramForArchive.Add(stripeTransferId);
                paramForArchive.Add(stripeBalanceTransaction);
                paramForArchive.Add(stripeDestinationAccount);
                paramForArchive.Add(stripeDestinationPaiement);
                paramForArchive.Add(stripeSourceTransaction);
                paramForArchive.Add(stripeTransferGroup);
                paramForArchive.Add(com_stripe);
                paramForArchive.Add(com_ct8);
                paramForArchive.Add(driver_net_price);

            }

            // On update la commande avec les nouveau chiffre
            var desPrice = (basePrice / 100).ToString("0.00").Replace(",", ".");

            var sqlUpdate = "UPDATE confirmation SET des_price='" + desPrice + "',com_stripe=NULL,com_ct8=NULL,driver_net_price=NULL WHERE  Id='" + orderId + "';";
            if (mySqlRequest.ExecuteNonQuery(sqlUpdate) == false) { Program.MainForm.WriteTextOnConsole("Mysql Error sql_update_confirmation"); }


            // Return des informations de Transaction pour l'archive de la commande dans la database.
            return paramForArchive;
        }

        public bool CradIsEuro(string countryCode)
        {
            Dictionary<string, string> countryArray = new Dictionary<string, string>();

            countryArray.Add("AT", "Austria");
            countryArray.Add("BE", "Belgium");
            countryArray.Add("BG", "Bulgaria");
            countryArray.Add("HR", "Croatia");
            countryArray.Add("CY", "Cyprus");
            countryArray.Add("CZ", "Czechia");
            countryArray.Add("DK", "Denmark");
            countryArray.Add("EE", "Estonia");
            countryArray.Add("FI", "Finland");
            countryArray.Add("FR", "France");
            countryArray.Add("DE", "Germany");
            countryArray.Add("EL", "Greece");
            countryArray.Add("HU", "Hungary");
            countryArray.Add("IE", "Ireland");
            countryArray.Add("IT", "Italy");
            countryArray.Add("LV", "Latvia");
            countryArray.Add("LT", "Lithuania");
            countryArray.Add("LU", "Luxembourg");
            countryArray.Add("MT", "Malta");
            countryArray.Add("NL", "Netherlands");
            countryArray.Add("PL", "Poland");
            countryArray.Add("PT", "Portugal");
            countryArray.Add("RO", "Romania");
            countryArray.Add("SK", "Slovakia");
            countryArray.Add("SI", "Slovenia");
            countryArray.Add("ES", "Spain");
            countryArray.Add("SE", "Sweden");
            countryArray.Add("UK", "United Kingdom");

            bool isEuropeUnion = countryArray.ContainsKey(countryCode);

            return isEuropeUnion;
        }


        public Object GetTarifs()
        {
            MySqlRequest mySqlRequest = new MySqlRequest();
            var sql_price = "SELECT * FROM tarification";
            var PriceValue = mySqlRequest.Select(sql_price);

            foreach (var val in PriceValue)
            {
                foreach (var finalVal in val)
                {
                    var tmpObj = new
                    {
                        finalVal

                    };
                }
            }

            //var jsonPrices = JsonConvert.SerializeObject(PriceValue);

            var jsonObj = JObject.FromObject(PriceValue[0]);

            var jsonPrice = new
            {
                one = JObject.FromObject(PriceValue[0]),
                two = JObject.FromObject(PriceValue[1]),
                three = JObject.FromObject(PriceValue[2]),
                four = JObject.FromObject(PriceValue[3])
            };


            return jsonPrice;
        }

        /// <summary>
        /// Stop le server
        /// </summary>
        public async void Stop()
        {
            await listener.StopListeningAsync();

            foreach (ITcpSocketClient s in SocketPool.Values.ToList())
            {
                s.Dispose();
                //var socketId = SocketPool.FirstOrDefault(x => x.Value == s).Key;
                //var clientId = socketId.Remove(0, 1);
                //SocketPool.Remove(socketId);
                //Program.MainForm.RemoveClientRow(clientId);
            }

            Program.MainForm.WriteTextOnConsole("STOPPED");
            Program.MainForm.ChangeStateOfStartButton(0);
            this.serverIsStarted = false;
            this.cleanUpTable = false;

        }

        /// <summary>
        /// retourne la varibale bolleen serverIsStarted
        /// </summary>
        /// <returns></returns>
        public bool IsServerStarted()
        {
            return this.serverIsStarted;
        }

        /// <summary>
        /// fonction qui retourne la distance en Metre entre deux coordonné géographique
        /// </summary>
        /// <param name="lat1">Latitude A</param>
        /// <param name="lng1">Longitude A</param>
        /// <param name="lat2">Latitude B</param>
        /// <param name="lng2">Longitude B</param>
        /// <returns></returns>
        private Double GetDistance(double lat1, double lng1, double lat2, double lng2)
        {
            var earth_radius = 6378137;   // Terre = sphère de 6378km de rayon
            var rlo1 = Deg2rad(lng1);
            var rla1 = Deg2rad(lat1);
            var rlo2 = Deg2rad(lng2);
            var rla2 = Deg2rad(lat2);
            var dlo = (rlo2 - rlo1) / 2;
            var dla = (rla2 - rla1) / 2;

            var a = (Math.Sin(dla) * Math.Sin(dla)) + Math.Cos(rla1) * Math.Cos(rla2) * (Math.Sin(dlo) * Math.Sin(dlo));
            var d = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            return (earth_radius * d);
        }

        /// <summary>
        /// fonction de convertion de degree à radian
        /// </summary>
        /// <param name="angle"></param>
        /// <returns></returns>
        private double Deg2rad(double angle)
        {
            return (Math.PI / 180) * angle;
        }

        public async void SendTestMsg()
        {
            var s = SocketPool.First(x => x.Key == "C1").Value;
            var t = s.GetStream();
            //var t = (System.Net.Sockets.TcpClient)s;

            var input = "OK";
            byte[] buffer = ASCIIEncoding.ASCII.GetBytes(input);

            t.Write(buffer, 0, buffer.Count());
            await t.FlushAsync();
        }

        private class ClientWriteSmartResult
        {
            public bool Result { get; set; }
            public Exception ExceptionDetails { get; set; }
        }

    }

}