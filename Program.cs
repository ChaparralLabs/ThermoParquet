
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
    public float intensity;
    public float? ion_mobility;
    public float? isolation_lower;
    public float? isolation_upper;
    public float? precursor_mz;
    public float? precursor_charge;
    //public string? filter;
}

class Program
{
    static async Task Main(string[] arg)
    {
        var path = arg[0];
        //var path = "C:\\Users\\Michael\\Documents\\2013_04_18_15_03_Q-Exactive-Orbitrap_1.raw";
        var output = path.Replace(".raw", ".mzparquet");
        var raw = RawFileReaderAdapter.FileFactory(path);
        raw.SelectInstrument(Device.MS, 1);
        int firstScanNumber = raw.RunHeaderEx.FirstSpectrum;
        int lastScanNumber = raw.RunHeaderEx.LastSpectrum;

        var data = new List<MzParquet>();

        ParquetSerializerOptions opts = new ParquetSerializerOptions();
        opts.CompressionMethod = Parquet.CompressionMethod.Zstd;
        opts.CompressionLevel = System.IO.Compression.CompressionLevel.Fastest;

        for (int scan = firstScanNumber; scan <= lastScanNumber; scan++)
        {

            var f = raw.GetFilterForScanNumber(scan);
            var rt = raw.RetentionTimeFromScanNumber(scan);

            ISimpleScanAccess cs = raw.GetSimplifiedCentroids(scan);

            if (cs.Masses.Length == 0)
            {
                cs = raw.GetSimplifiedScan(scan);
            }

            float? isolation_lower = null;
            float? isolation_upper = null;
            float? precursor_mz = null;
            float? precursor_charge = null;


            if ((int)f.MSOrder > 1)
            {
                var rx = f.GetReaction(0);
                isolation_lower = (float)(rx.PrecursorMass - rx.IsolationWidth / 2);
                isolation_upper = (float)(rx.PrecursorMass + rx.IsolationWidth / 2);
                precursor_mz = (float)rx.PrecursorMass;
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

                if (trailer.Labels[i].StartsWith("Charge"))
                {
                    var val = float.Parse(trailer.Values[i]);
                    if (val > 0)
                    {
                        precursor_charge = val;
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
                m.intensity = (float) cs.Intensities[i];
                m.mz = (float) cs.Masses[i];
                m.isolation_lower = isolation_lower;
                m.isolation_upper = isolation_upper;
                m.precursor_mz = precursor_mz;
                m.precursor_charge = precursor_charge;
                m.ion_mobility = null;
                //m.filter = filter;

                data.Add(m);
            }

            if (data.Count >= 1048576)
            {
                await ParquetSerializer.SerializeAsync(data, output, opts);
                opts.Append = true;
                data.Clear();
                Console.WriteLine("writing chunk");
            }
        }

        if (data.Count > 0)
        {
            await ParquetSerializer.SerializeAsync(data, output, opts);
            Console.WriteLine("writing chunk");
        }
        Console.WriteLine("finished writing to {0}", output);
    }
}

