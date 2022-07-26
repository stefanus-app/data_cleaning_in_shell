#!/bin/bash


# melakukan penggabungan 2 file sample 2019 Nov dan Okt
gabung_csv () {
    data_gabung=$(csvstack $1 $2)
    echo "$data_gabung"
}

# slicing hanya kolom yang dibutuhkan saja
slice_data () {
    sliced_data=$(csvcut -c event_time,event_type,product_id,category_id,brand,price,category_code $1 | csvgrep -c event_type -m purchase)
    echo "$sliced_data"
}

# piping 2 fungsi yang sudah dibuat
gabung_csv 2019-Nov-sample.csv 2019-Oct-sample.csv | slice_data > sliced_data.csv

# mengganti nama kolom category_code menjadi 3 bagian dengan . sebagai delimiter
cat sliced_data.csv | sed -Ee 's/(.*)category_code/\1category.cat2.product_name/' > splitted_column.csv

# merubah semua karakter . menjadi , di kolom ke 7
awk -F, 'BEGIN{FS=OFS=","} {gsub(/\./,",",$7); print}' splitted_column.csv > splitted_data.csv

# mengisi null value pada kolom product_name menjadi value dari kolom sebelumnya (cat2)
csvcut -c event_time,event_type,product_id,category_id,brand,price,category,cat2,product_name,cat2 splitted_data.csv > splitted_data2.csv
awk -F, 'BEGIN{FS=OFS=","} {for(i=1;i<NF;i++){if($i==""){$i=l}else{l=$i}}print}' splitted_data2.csv > all_cleaned.csv

csvcut -C 8,10 all_cleaned.csv > result.csv

rm {sliced_data,splitted_column,splitted_data,splitted_data2,all_cleaned}.csv

head result.csv | csvlook