# ThermoParquet

### Testing docker file

```sh
# assumes you have ~/Downloads/my_data.raw
docker build . -t pqt
docker run \
  -v ~/Downloads/:/mnt/data \
  pqt /mnt/data/my_data.raw
 
```
