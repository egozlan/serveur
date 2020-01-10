using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Stripe;

namespace OokcityServer
{
    class StripeController
    {
        /// <summary>
        /// Makes the one click payment.
        /// </summary>
        /// <returns>The one click payment.</returns>
        /// <param name="customerId">Customer identifier.</param>
        /// <param name="amount">Amount.</param>
        /// <param name="curency">Curency.</param>
        public static Charge MakeOneClickPayment(string customerId, int amount, string curency = "eur")
        {
            StripeConfiguration.ApiKey = "sk_test_dBR2X5yRuQklppKxRz7jCbuT";

            var chargeOptions = new ChargeCreateOptions()
            {
                Amount = amount,
                Currency = curency,
                Capture = false,
                Customer = customerId
               // CustomerId = customerId
            };
            var chargeService = new ChargeService();
            Charge charge = chargeService.Create(chargeOptions);

            return charge;

        }

        public static Charge MakeCapture(string customerId,string chargeId,string orderId,string clientId)
        {
            StripeConfiguration.ApiKey = "sk_test_dBR2X5yRuQklppKxRz7jCbuT";

            var chargeUpdateOptions = new ChargeUpdateOptions()
            {
                Description = orderId
            };

            var chargeService = new ChargeService();
            chargeService.Update(chargeId, chargeUpdateOptions);


            Charge charge = chargeService.Capture(chargeId, null);

            return charge;
        }

    }

    

}
