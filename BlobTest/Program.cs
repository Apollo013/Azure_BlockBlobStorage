using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Configuration;
using System.IO;
using System.Linq;


namespace BlobTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create a refrence to our cloud storage
            // We do this by grabbing the name and key details of the storage account from our App.Config file
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings.Get("StorageConnectionString"));

            // Create a blob client that will allow us to retrieve objects that represent containers and blobs 
            // stored within the Blob Storage Service
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Create a new blob container
            CloudBlobContainer container = CreateContainer(blobClient, "test-container");

            // Create 2 blobs from the same file
            string filename = @"C:\App Tests\Azure\BlobTest\BlobTest\TextFile1.txt";
            UploadBlob(container, filename, "blobtest");
            UploadBlob(container, filename);

            // List all blobs in this container (should be 2)
            ListBlockBlobs(container);

            // Download blob
            string newFilename = @"C:\App Tests\Azure\BlobTest\BlobTest\TextFile2.txt";
            DownloadBlockBlob(container, "blobtest", newFilename);

            // Delete blob
            DeleteBlockBlob(container, "blobtest");

            // List all blobs in this container (should be 1)
            ListBlockBlobs(container);
        }


        private static CloudBlobContainer CreateContainer(CloudBlobClient blobClient, string containerName, bool makePublic = false)
        {
            printHeader(String.Format("Create Container: {0}", containerName));

            // Create a reference to a particular blob container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExists();

            if (makePublic)
            {
                container.SetPermissions(new BlobContainerPermissions{PublicAccess = BlobContainerPublicAccessType.Blob});
            }

            return container;
        }


        private static CloudBlobContainer GetContainer(CloudBlobClient blobClient, string containerName)
        {
            printHeader(String.Format("Get Container: {0}", containerName));

            // Create a reference to a particular blob container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            return container;
        }


        private static void UploadBlob(CloudBlobContainer container, string filename, string blockReference)
        {
            printHeader(String.Format("Uploading Block Blob: {0}", blockReference));

            // Retrieve reference to a blob
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blockReference);

            // Create or overwrite the blob with contents from a local file.
            using (var fileStream = File.OpenRead(filename))
            {
                blockBlob.UploadFromStream(fileStream);
            }
        }

        private static void UploadBlob(CloudBlobContainer container, string filename)
        {
            // Get the short name of the file, this will be used to reference the blob.
            string shortfilename = Path.GetFileName(filename);
            printHeader(String.Format("Uploading Block Blob: {0}", shortfilename));

            // Retrieve reference to a blob
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(shortfilename);

            // Create or overwrite the blob with contents from a local file.
            using (var fileStream = File.OpenRead(filename))
            {
                blockBlob.UploadFromStream(fileStream);
            }
        }


        private static bool checkFile(string filename)
        {
            return File.Exists(filename);
        }


        private static void ListBlockBlobs(CloudBlobContainer container)
        {
            printHeader(String.Format("Listing Block Blobs in {0}", container.Name));

            var blobs = container.ListBlobs(null, true);
            if (blobs.Count() == 0)
            {
                Console.WriteLine("There are no blobs to list\n");
            }
            else
            {
                // Loop over items within the container and output the length and URI.
                foreach (IListBlobItem item in container.ListBlobs(null, true))
                {
                    // Check for each type of blob and output details to console
                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        CloudBlockBlob blob = (CloudBlockBlob)item;

                        Console.WriteLine("Block blob of length {0}: {1}", blob.Properties.Length, blob.Uri);

                    }
                }
            }

        }


        private static void DownloadBlockBlob(CloudBlobContainer container, string blockReference, string filename)
        {
            printHeader(String.Format("Downloading Block Blob: {0}", blockReference));

            // Retrieve reference to a named blob.
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blockReference);

            // Save blob contents to a file.
            using (var fileStream = System.IO.File.OpenWrite(filename))
            {
                blockBlob.DownloadToStream(fileStream);
                Console.WriteLine("Downloaded to: {0}\n", filename);
            }
        }


        private static void DeleteBlockBlob(CloudBlobContainer container, string blockReference)
        {
            printHeader(String.Format("Deleting Block Blob: {0}", blockReference));

            // Retrieve reference to a named blob.
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blockReference);

            // Delete the blob.
            blockBlob.Delete();
        }


        private static void printHeader(string title)
        {
            Console.WriteLine("\n=============================================================");
            Console.WriteLine("{0}", title);
            Console.WriteLine("=============================================================");
        }

    }
}
