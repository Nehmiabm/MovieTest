using System;
using System.Collections.Generic;
using System.Data.Common;
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
using System.Data.Odbc;

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

        private static async void RecommendMovies()
        {
           //IDataModel model = new FileDataModel(@"C:\bigdata\MovieTweetings-master\latest\ratings2.csv");
           Console.WriteLine("Building model...");
            
             IDataModel model = await GetMovieDataModel();
            Console.WriteLine("Building model done!");
            Console.WriteLine("Calculating Recommendation...");
            //Creating UserSimilarity object.
            IUserSimilarity usersimilarity = new LogLikelihoodSimilarity(model);

            //Creating UserNeighbourHHood object.
            IUserNeighborhood userneighborhood = new NearestNUserNeighborhood(15, usersimilarity, model);

            //Create UserRecomender
            IUserBasedRecommender recommender = new GenericUserBasedRecommender(model, userneighborhood, usersimilarity);

            var recommendations = recommender.Recommend(2, 10);

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


        public static async Task<IDataModel> GetMovieDataModel()
        {
            IDataModel model = null;
            using (OdbcConnection conn =
                   new OdbcConnection(connectionString: "DSN=Sample Microsoft Hive DSN;UID=admin;PWD=Password@123"))
            {
                FastByIDMap<IPreferenceArray> preferences = new FastByIDMap<IPreferenceArray>();
                conn.OpenAsync().Wait();
                OdbcCommand ratingCommand = conn.CreateCommand();
                ratingCommand.CommandText = "SELECT * FROM rating;";
               
                DbDataReader ratingReader = await ratingCommand.ExecuteReaderAsync();
                
                Console.WriteLine("...........................................");
                int userID = 0;
                int loop = 0;
                List<object[]> templist = new List<object[]>();
                while (ratingReader.Read())
                {
                    object[] uval = new object[3];
                    uval[0] = ratingReader.GetInt32(0); //user
                    uval[1] = ratingReader["movieid"]; //movieid
                    uval[2] = ratingReader.GetInt32(2); //rating


                    if (userID != ratingReader.GetInt32(0) && loop++ != 0)
                    {
                        IPreferenceArray usePref = new GenericUserPreferenceArray(templist.Count);
                        int j = 0;
                        foreach (var urate in templist)
                        {
                            if (j == 0)
                                usePref.SetUserID(0, Convert.ToInt32(urate[0]));
                            usePref.SetItemID(j, Convert.ToInt64(urate[1]));
                            usePref.SetValue(j, Convert.ToInt64(urate[2]));
                            j++;
                        }

                        preferences.Put(userID, usePref);
                        templist = new List<object[]>();
                    }
                    else
                    {
                        templist.Add(uval);
                    }

                    userID = ratingReader.GetInt32(0);
                   // Console.WriteLine(userReader.GetInt32(0) + ".........  " + userReader.GetString(1));
                }

                if (templist.Count > 0)
                {
                    IPreferenceArray usePref = new GenericUserPreferenceArray(templist.Count);
                    int k = 0;
                    foreach (var urate in templist)
                    {
                        if (k == 0)
                            usePref.SetUserID(0, Convert.ToInt32(urate[0]));
                        usePref.SetItemID(k, Convert.ToInt64(urate[1]));
                        usePref.SetValue(k, Convert.ToInt64(urate[2]));
                        k++;
                    }

                    preferences.Put(userID, usePref);
                }

                 model=new GenericDataModel(preferences);
            
            }
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
