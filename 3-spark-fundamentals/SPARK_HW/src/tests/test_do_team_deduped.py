from chispa.dataframe_comparer import *
from ..jobs.team_deduped_job import do_team_deduped
from collections import namedtuple

Teams = namedtuple("Teams" , "tema_id" "city" "team_wins" "team_losts")
Teams_deduped = namedtuple("Teams_deduped" ,"team_id" , "city" , "team_wins" , "team_losts" , "row_num")


def test_do_team_deduped(spark) :
    source_data = [
        Teams("141578" , "LA" , "48" , "52") ,
        Teams("459826" , "CA" , "79" , "21") ,
        Teams("141578" , "LA" , "48" , "52") ,
        Teams("141578" , "LA" , "48" , "52") ,
        Teams("254781" , "TA" , "99" , "1")
    ]

    source_df = spark.createDataFrame(source_data)
    actual_df = do_player_scd_transformation(spark, source_df)
    
    expected_data = [
        Teams_deduped("141578", 'LA', "48", "52" , 3),
        Teams_deduped("459826", 'CA', "79", "21" , 1),
        Teams_deduped("254781", 'TA', "99", "1" , 1)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)