# ThermoParquet

### Testing docker file

```sh
# assumes you have ~/Downloads/my_data.raw
docker build . -t pqt
docker run -v ~/Downloads/:/mnt/data pqt /mnt/data/my_data.raw
 
```

example.
sudo docker run -v /home/cpl/data/HLA/:/mnt/data pqt /mnt/data/YE_20180507_SK_HLA_B4001_3Ips_a50mio_R1_01.raw
sudo docker run -v /home/cpl/data/HLA/:/mnt/data pqt /mnt/data/YE_20180507_SK_HLA_B4001_3Ips_a50mio_R1_02.raw
sudo docker run -v /home/cpl/data/HLA/:/mnt/data pqt /mnt/data/YE_20180507_SK_HLA_B4001_3Ips_a50mio_R2_01.raw
sudo docker run -v /home/cpl/data/HLA/:/mnt/data pqt /mnt/data/YE_20180507_SK_HLA_B4001_3Ips_a50mio_R2_02.raw

