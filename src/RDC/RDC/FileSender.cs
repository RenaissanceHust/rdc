using System.IO;
using System.Security.Cryptography;

namespace RDC
{
    /// <summary>
    /// This algorithm is a variant of the third (and final) deltas transfer algorithm described in
    /// the PhD thesis of the RSYNC application author. This algorihtm uses the MD5 cryptografic hash
    /// function and a rolling checksum hash function. Each file transfer takes makes a maximum of
    /// two passes on the original file. This file transfer algorithm is <b>not</b> compatible with
    /// the RSYNC algorihtm described in the paper. Maximum file size of 4GiB.
    /// 
    /// This algorithm does a lot of work in the receiving end, in order to avoid timeouts the network
    /// stream timeout values are temporally increased. The network stream provided must support this
    /// behaviour for the transfer of bigger files.
    /// </summary>
    public static class FileSender
    {
        private const int BlockSize = 5205; // RSYNC fixed block size
        private const int BufferSize = 65536; // Actual useful buffer size
        private static readonly MD5CryptoServiceProvider Md5 = new MD5CryptoServiceProvider();
        /// <summary>
        /// RSYNC file transfer with exchange of delta representatives (sending end)
        /// </summary>
        /// <param name="stream">Connection stream</param>
        /// <param name="path">Path of file to transfer, must exist</param>
        public static void Send(Stream stream, string path)
        {
            var rollingChecksum = new C3C4TaylorsRollingChecksum(stream, BlockSize);
            var fileSize = (int)new FileInfo(path).Length;
            int prevReadTimeout = stream.ReadTimeout;
            int prevWriteTimeout = stream.WriteTimeout;
            stream.ReadTimeout = stream.WriteTimeout = fileSize;
            var buffer = new byte[BufferSize];
            // Send hash of whole file
            try{
                using (var fileStream = new FileStream(path, FileMode.Open, FileAccess.Read))
                {
                    // Send hash of whole file
                    stream.Write(Md5.ComputeHash(fileStream), 0, 16);
                    fileStream.Seek(0, 0);
                    stream.WriteInt(fileSize);
                    GenerateAndSendHashes(stream, rollingChecksum, fileStream, fileSize, buffer);
                    ReceiveChunkInfoAndSendChunks(stream, fileStream, buffer);
                }
            }
            finally
            {
                stream.ReadTimeout = prevReadTimeout;
                stream.WriteTimeout = prevWriteTimeout;
            }
        }
        private static void GenerateAndSendHashes(Stream stream, C3C4TaylorsRollingChecksum rollingChecksum, Stream fileStream, int fileSize, byte[] buffer)
        {
            int fileSize2 = fileSize;
            int weakHash = 0;
            while (fileSize2 > 0)
            {
                int toSend = fileSize2 < BlockSize ? fileSize2 : BlockSize;
                for (int i = 0; i < toSend; ++i)
                    weakHash = rollingChecksum.Slide();
                stream.WriteInt(weakHash);
                fileStream.Seek(fileSize - fileSize2, 0);
                fileStream.ForceRead(buffer, 0, toSend);
                byte[] strongHash = Md5.ComputeHash(buffer, 0, toSend);
                stream.Write(strongHash, 0, strongHash.Length);
                stream.WriteInt(toSend);
                fileSize2 -= toSend;
            }
        }
        private static void ReceiveChunkInfoAndSendChunks(Stream stream, Stream fileStream, byte[] buffer)
        {
            int notFoundIntervals = stream.ReadInt();
            while (notFoundIntervals-- > 0)
            {
                int start = stream.ReadInt();
                int length = stream.ReadInt();
                fileStream.Seek(start, 0);
                fileStream.ReadWrite(stream, length, buffer);
            }
        }
    }
}
