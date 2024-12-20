# $VDB_HOME is home directory path of vdb(redis)
CLIENT_PATH=$VDB_HOME/client.py
function single_type_columns() {
  list=`cat $VDB_HOME/tests/ddl/batchinsert/single_type.columns`
  echo  $list 
}
function list_type_columns() {
  list=`cat $VDB_HOME/tests/ddl/batchinsert/list_type.columns`
  echo  $list 
}
function fixed_list_type_columns() {
  list=`cat $VDB_HOME/tests/ddl/batchinsert/fixed_list_type.columns`
  echo  $list 
}
#drop exist tables
python $CLIENT_PATH drop "single"
python $CLIENT_PATH drop "list"
python $CLIENT_PATH drop "fixed_list"

#create tables
python $CLIENT_PATH create "single" "id int32, $(single_type_columns)" "segment_id_info:id"
python $CLIENT_PATH create "list" "id int32, $(list_type_columns)" "segment_id_info:id"
python $CLIENT_PATH create "fixed_list" "id int32, $(fixed_list_type_columns)" "segment_id_info:id, ann_column_id:10, index_space:L2Space, ef_construction:50, M:4, active_set_size_limit:10, index_type:HNSW"

#test example 

##batchinsert small
#python $CLIENT_PATH insertcsv "single" $VDB_HOME/tests/csv/batchinsert/single_10.csv 
#python $CLIENT_PATH insertcsv "list" $VDB_HOME/tests/csv/batchinsert/list_10.csv 
#python $CLIENT_PATH insertcsv "fixed_list" $VDB_HOME/tests/csv/batchinsert/fixed_list_10.csv 
#
##batchinsert medium
#python $CLIENT_PATH insertcsv "single" $VDB_HOME/tests/csv/batchinsert/single_100.csv 
#python $CLIENT_PATH insertcsv "list" $VDB_HOME/tests/csv/batchinsert/list_100.csv 
#python $CLIENT_PATH insertcsv "fixed_list" $VDB_HOME/tests/csv/batchinsert/fixed_list_100.csv 
#
##batchinsert large
#python $CLIENT_PATH insertcsv "single" $VDB_HOME/tests/csv/batchinsert/single_10000.csv 
#python $CLIENT_PATH insertcsv "list" $VDB_HOME/tests/csv/batchinsert/list_10000.csv 
#python $CLIENT_PATH insertcsv "fixed_list" $VDB_HOME/tests/csv/batchinsert/fixed_list_10000.csv 
