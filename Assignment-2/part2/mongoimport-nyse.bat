cd data/NYSE
for %%f in ("NYSE_daily_prices*") do mongoimport --db nyse --collection stocks --file %%f --type csv --headerline
cd ../../