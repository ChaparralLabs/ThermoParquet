
using System.Formats.Tar;
using Parquet.Serialization;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;

struct MzParquet
{
    public uint scan;
    public uint level;
    public float rt;
    public float mz;
    public uint intensity;
    public float? ion_mobility;
    public float? isolation_lower;
    public float? isolation_upper;
    public uint? precursor_scan;
    public float? precursor_mz;
    public uint? precursor_charge;
    public float? precursor_intensity;
    //public string? filter;
}

class Program
{
    static async Task Main(string[] args)
    {
        // Check if a file path was provided
        if (args.Length == 0)
        {
            Console.WriteLine("ERROR: No input file specified.");
            Console.WriteLine("");
            Console.WriteLine("Usage: ThermoParquet.exe <path_to_raw_file>");
            Console.WriteLine("");
            Console.WriteLine("Example:");
            Console.WriteLine("  ThermoParquet.exe C:\\Data\\sample.raw");
            Console.WriteLine("");
            Console.WriteLine("This will create: C:\\Data\\sample.mzparquet");
            Environment.Exit(1);
            return;
        }

        var path = args[0];

        // Check if file exists
        if (!File.Exists(path))
        {
            Console.WriteLine($"ERROR: File not found: {path}");
            Console.WriteLine("");
            Console.WriteLine("Please check that the file path is correct and the file exists.");
            Environment.Exit(1);
            return;
        }

        // Check if it's a .raw file
        if (!path.EndsWith(".raw", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"ERROR: Input file must be a .raw file: {path}");
            Console.WriteLine("");
            Console.WriteLine("This tool only converts Thermo .raw files to .mzparquet format.");
            Environment.Exit(1);
            return;
        }

        var output = path.Replace(".raw", ".mzparquet", StringComparison.OrdinalIgnoreCase);
        
        // Check if output directory is writable (handles network drives, read-only folders, etc.)
        var outputDir = Path.GetDirectoryName(output) ?? ".";
        bool useLocalFallback = false;
        string? localFallbackDir = null;
        
        if (!IsDirectoryWritable(outputDir))
        {
            useLocalFallback = true;
            localFallbackDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "mzparquet_output");
            Directory.CreateDirectory(localFallbackDir);
            output = Path.Combine(localFallbackDir, Path.GetFileName(output));
            Console.WriteLine($"WARNING: Cannot write to source directory (network drive or read-only).");
            Console.WriteLine($"         Output will be saved to: {localFallbackDir}");
            Console.WriteLine("");
        }
        
        var tempOutput = output + ".tmp";
        
        Console.WriteLine($"Converting: {path}");
        Console.WriteLine($"Output: {output}");
        Console.WriteLine("");

        IRawDataPlus? raw = null;
        try
        {
            raw = RawFileReaderAdapter.FileFactory(path);
            
            if (raw == null || raw.IsError)
            {
                Console.WriteLine("ERROR: Failed to open the .raw file.");
                Console.WriteLine("");
                Console.WriteLine("The file may be corrupted or not a valid Thermo raw file.");
                Environment.Exit(1);
                return;
            }

            raw.SelectInstrument(Device.MS, 1);
            int firstScanNumber = raw.RunHeaderEx.FirstSpectrum;
            int lastScanNumber = raw.RunHeaderEx.LastSpectrum;
            
            Console.WriteLine($"Processing {lastScanNumber - firstScanNumber + 1} scans...");

        var data = new List<MzParquet>();

        ParquetSerializerOptions opts = new ParquetSerializerOptions();
        opts.CompressionMethod = Parquet.CompressionMethod.Zstd;
        opts.CompressionLevel = System.IO.Compression.CompressionLevel.Fastest;

        var last_scans = new Dictionary<int, uint>();
        int totalScans = lastScanNumber - firstScanNumber + 1;
        int lastReportedProgress = 0;

        for (int scan = firstScanNumber; scan <= lastScanNumber; scan++)
        {
            // Report progress every 2%
            int progress = (scan - firstScanNumber) * 100 / totalScans;
            if (progress >= lastReportedProgress + 2)
            {
                lastReportedProgress = (progress / 2) * 2;
                Console.WriteLine($"Progress: {lastReportedProgress}%");
            }

            var f = raw.GetFilterForScanNumber(scan);
            var rt = raw.RetentionTimeFromScanNumber(scan);
            last_scans[(int) f.MSOrder] = (uint) scan;

            ISimpleScanAccess cs = raw.GetSimplifiedCentroids(scan);

            if (cs.Masses.Length == 0)
            {
                cs = raw.GetSimplifiedScan(scan);
            }

            float? isolation_lower = null;
            float? isolation_upper = null;
            uint? precursor_scan = null;
            float? precursor_mz = null;
            uint? precursor_charge = null;
            float? precursor_intensity = null;



            if ((int)f.MSOrder > 1)
            {
                var rx = f.GetReaction(0);
                isolation_lower = (float)(rx.PrecursorMass - rx.IsolationWidth / 2);
                isolation_upper = (float)(rx.PrecursorMass + rx.IsolationWidth / 2);
                precursor_mz = (float)rx.PrecursorMass;
                uint t;
                if (last_scans.TryGetValue((int)f.MSOrder - 1, out t))
                {
                    precursor_scan = t;
                    //Console.WriteLine("Previous scan for MS${0}:{1} = {2} {3}", f.MSOrder, scan, t, f.ToString());
                }
            }

            var trailer = raw.GetTrailerExtraInformation(scan);

            for (var i = 0l; i < trailer.Length; i++)
            {

                if (trailer.Labels[i].StartsWith("Monoisotopic M/Z"))
                {
                    var val = float.Parse(trailer.Values[i]);
                    if (val > 0)
                    {
                        precursor_mz = val;
                    }
                }

                // FIXME: handle cases where this doesn't exist?
                if (trailer.Labels[i].StartsWith("Master Scan"))
                {
                    var val = Int64.Parse(trailer.Values[i]);
                    if (val > 0)
                    {
                        precursor_scan = (uint) val;
                       // Console.WriteLine("Previous scan for MS${0}:{1} = {2} {3}", f.MSOrder, scan, precursor_scan, f.ToString());

                    }
                }

                if (trailer.Labels[i].StartsWith("Charge"))
                {
                    var val = uint.Parse(trailer.Values[i]);
                    if (val > 0)
                    {
                        precursor_charge = val;
                    }
                }

                if ((int)f.MSOrder > 1 && trailer.Labels[i].StartsWith("Intensity"))
                {
                    if (float.TryParse(trailer.Values[i], out var val) && val > 0)
                    {
                        precursor_intensity = val;
                    }
                }
            }

            //var filter = f.ToString();
            for (int i = 0; i < cs.Masses.Length; i++)
            {
                MzParquet m;
                m.rt = (float)rt;
                m.scan = (uint)scan;
                m.level = ((uint)f.MSOrder);
                m.intensity = (uint) cs.Intensities[i];
                m.mz = (float) cs.Masses[i];
                m.isolation_lower = isolation_lower;
                m.isolation_upper = isolation_upper;
                m.precursor_scan = precursor_scan;
                m.precursor_mz = precursor_mz;
                m.precursor_charge = precursor_charge;
                m.precursor_intensity = precursor_intensity;
                m.ion_mobility = null;
                //m.filter = filter;

                data.Add(m);
            }

            if (data.Count >= 1048576)
            {
                await ParquetSerializer.SerializeAsync(data, tempOutput, opts);
                opts.Append = true;
                data.Clear();
            }
        }

        if (data.Count > 0)
            {
                await ParquetSerializer.SerializeAsync(data, tempOutput, opts);
                Console.WriteLine("Writing final chunk...");
            }

            // Atomic rename: replace output with completed temp file
            if (File.Exists(output))
                File.Delete(output);
            File.Move(tempOutput, output);
            
            Console.WriteLine("");
            Console.WriteLine($"SUCCESS: Conversion complete!");
            Console.WriteLine($"Output file: {output}");
        }
        catch (Exception ex)
        {
            Console.WriteLine("");
            Console.WriteLine($"ERROR: Conversion failed.");
            Console.WriteLine("");
            Console.WriteLine($"Details: {ex.Message}");
            Console.WriteLine("");
            Console.WriteLine("Common causes:");
            Console.WriteLine("  - The .raw file is corrupted or incomplete");
            Console.WriteLine("  - The file is being used by another program");
            Console.WriteLine("  - Insufficient disk space for output file");
            Console.WriteLine("  - No write permission to output folder");
            // Clean up partial temp file on failure
            if (File.Exists(tempOutput))
            {
                try { File.Delete(tempOutput); } catch { }
            }
            Environment.Exit(1);
        }
        finally
        {
            raw?.Dispose();
        }
    }
    
    /// <summary>
    /// Check if a directory is writable by attempting to create a temp file.
    /// This handles network drives, read-only folders, permission issues, etc.
    /// </summary>
    static bool IsDirectoryWritable(string dirPath)
    {
        try
        {
            // Try to create a temporary file to verify write access
            string testFile = Path.Combine(dirPath, $".write_test_{Guid.NewGuid():N}.tmp");
            using (var fs = File.Create(testFile, 1, FileOptions.DeleteOnClose))
            {
                // File created successfully, directory is writable
            }
            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
        catch (Exception)
        {
            return false;
        }
    }
}

