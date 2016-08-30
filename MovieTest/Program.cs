using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using INTAPS.RDBMS;
using Microsoft.Hadoop.Hive;
using Microsoft.WindowsAzure;
using NReco.CF.Taste.Impl.Common;
using NReco.CF.Taste.Impl.Model;
using NReco.CF.Taste.Impl.Model.File;
using NReco.CF.Taste.Impl.Neighborhood;
using NReco.CF.Taste.Impl.Recommender;
using NReco.CF.Taste.Impl.Similarity;
using NReco.CF.Taste.Model;
using NReco.CF.Taste.Neighborhood;
using NReco.CF.Taste.Recommender;
using NReco.CF.Taste.Similarity;

namespace MovieTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // ConvertMovieData();
            RecommendMovies();
         //   ReadFromHDInsightCluster();
       //  GetMovieDataModel();

        }

        private static void ConvertMovieData()
        {
            try
            {
                string path = @"C:\bigdata\MovieTweetings-master\latest";
                StringBuilder builder = new StringBuilder();
                string line;

                Console.WriteLine("Converting data. Please wait...");
                // Read the file ine by line.
                System.IO.StreamReader file =
                    new System.IO.StreamReader(path + @"\ratings.dat");
                while ((line = file.ReadLine()) != null)
                {
                    string str = line.Replace("::", ",");
                    string[] s = str.Split(',');
                    string newStr = s[0] + "," + s[1] + "," + s[2];
                    builder.AppendLine(newStr);
                }

                file.Close();
                System.IO.File.WriteAllText(path + @"\ratings2", builder.ToString());
                Console.WriteLine("rating data successfully converted");
                Console.ReadKey();
            }
            catch (IOException ex)
            {

                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }
        }

        private static void RecommendMovies()
        {
           IDataModel model = new FileDataModel(@"C:\bigdata\MovieTweetings-master\latest\ratings2.csv");
           Console.WriteLine("Building model...");
            //IDataModel model = GetMovieDataModel();
            Console.WriteLine("Building model done!");
            Console.WriteLine("Calculating Recommendation...");
            //Creating UserSimilarity object.
            IUserSimilarity usersimilarity = new LogLikelihoodSimilarity(model);

            //Creating UserNeighbourHHood object.
            IUserNeighborhood userneighborhood = new NearestNUserNeighborhood(15, usersimilarity, model);

            //Create UserRecomender
            IUserBasedRecommender recommender = new GenericUserBasedRecommender(model, userneighborhood, usersimilarity);

            var recommendations = recommender.Recommend(2, 3);

            foreach (IRecommendedItem recommendation in recommendations)
            {
                Console.WriteLine(recommendation);
            }
            Console.WriteLine("Calculation done!");
            Console.ReadLine();


        }

        public static void ReadFromHDInsightCluster()
        {

        

            var db = new HiveDatabase(
                webHCatUri: new Uri("https://myhdicluster.azurehdinsight.net"),
                username: "admin", password: "Password@123",
                azureStorageAccount: "adminds2.blob.core.windows.net",
                azureStorageKey:
                    "Rw85kKqrtjOxmjxwPF3pGCPnJPAy60GnMa9ZphrlM8yg2mzA8ENHsIiPFx8V0PaOji49dImpafAsiI6MXKS+Eg==");

            var q = from u in db.users
                where u.userID == 1
                select u;

            q.ExecuteQuery().Wait();

            var results = q.ToList();
            foreach (var r in results)
            {
                Console.WriteLine("User ID:" + r.userID + " Twitter ID:" + r.twitterID);
            }
            Console.WriteLine("---------------------------------");
            Console.WriteLine("Press a key to end");
            Console.Read();
        }


        public class HiveDatabase : Microsoft.Hadoop.Hive.HiveConnection
        {
            public HiveDatabase(Uri webHCatUri, string username, string password,
                string azureStorageAccount, string azureStorageKey)
                : base(webHCatUri, username, password)
            {

            }

            public HiveTable<UserRow> users
            {
                get { return this.GetTable<UserRow>("users"); }
            }
        }

        public class UserRow : HiveRow
        {
            public int userID { get; set; }
            public long twitterID { get; set; }
        }


        public static IDataModel GetMovieDataModel()
        {
            FastByIDMap<IPreferenceArray> preferences = new FastByIDMap<IPreferenceArray>();

            string conn = "Server=tcp:sqldatabasemovie.database.windows.net,1433;Initial Catalog=MovieDB;Persist Security Info=False;User ID=nehmia;Password=Password@123;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
           INTAPS.RDBMS.DSP DBConnection = new INTAPS.RDBMS.DSP(INTAPS.RDBMS.DBProvider.MSSQLSERVER)
            { ConnectionString = conn };

          //  Console.WriteLine("Building model please wait...");

            var users = DBConnection.GetSTRArrayByFilter<Users>("userID<200");
           
            foreach (Users user in users)
            {
                var userRatings = DBConnection.GetSTRArrayByFilter<Rating>("userID=" + user.userID);
                IPreferenceArray usePref = new GenericUserPreferenceArray(userRatings.Length);
                int i = 0;
                foreach (Rating rating in userRatings)   //build preferences
                {
                    if (i == 0)
                        usePref.SetUserID(i, rating.userID);
                    usePref.SetItemID(i,rating.movieID);
                    usePref.SetValue(i, rating.rating);
                    i++;
                }
                preferences.Put(user.userID, usePref);
            }

            IDataModel model= new GenericDataModel(preferences);

            return model;
        }

        [SingleTableObject]
        public class Users
        {
            [IDField]
            public int userID;
            [DataField]
            public long twitterID;
        }

        [SingleTableObject]
        public class Movie
        {
            [IDField] 
            public long movieID;

            [DataField] 
            public string title;

            [DataField] 
            public string genre;

        }

        [SingleTableObject]
        public class Rating
        {
         
            [DataField]
            public int userID;
            [DataField]
            public long movieID;
            [DataField]
            public int rating;

        }
    }
}
