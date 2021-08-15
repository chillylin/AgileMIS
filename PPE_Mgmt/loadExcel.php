<?php 

require_once('./private/initialize.php');
require_once('SimpleXLSX.php');


function insert_codes($handler, $db , $tablename, $tablecol1, $tablecol2){

    $cleanquery = 'DELETE FROM '.$tablename;
    if (mysqli_query($db,$cleanquery)){
        //echo $tablename.' is cleaned';
    }
    else{
        echo $cleanquery."<br>";
        echo $tablename."' cleaning failed<br><br>";
    };

    
    $key = array_search($tablename, $handler->sheetNames());


    $query = "INSERT INTO ".$tablename." (". $tablecol1.",".$tablecol2.") VALUES ";

    foreach( $handler->rows($key) as $row){
        $query .= "('".$row[0]."','".$row[1]."'), ";
    }

    $query = substr( $query, 0, -2).';';

    if (mysqli_query($db,$query)){
        //echo $tablename.' data is successfully inserted';
    }
    else{
        echo $query."<br>";
        echo $tablename."data is not inserted<br><br>";
    };

}

function insert_records($handler, $db , $tablename){

    $key = array_search($tablename, $handler->sheetNames());
    
    $cleanquery = 'DELETE FROM '.$tablename;
    if (mysqli_query($db,$cleanquery)){
        //echo $tablename.' is cleaned';
    }
    else{
        echo $cleanquery."<br>";
        echo $tablename."' cleaning failed<br><br>";
    };

    
    $firstflag = 1;
    foreach(  $handler->rows($key) as $row){

        if (1 == $firstflag){
            $firstflag = 0;
            $query = "INSERT INTO ".$tablename." (";
            foreach($row as $cell){
                $query .= $cell.", ";
            }
            $query = substr( $query, 0, -2);
            $query .= ") VALUES ";
        }
        else {
            $query .= "(";
            foreach($row as $cell){
                

                $query .= "'".$cell."', ";
            }
            $query = substr( $query, 0, -2);
            $query .= "), ";
        }
        
    }
    $query = substr( $query, 0, -2).';';
   
    if (mysqli_query($db,$query)){
        //echo $tablename.' data is successfully inserted';
    }
    else{
        echo $query."<br>";
        echo $tablename."data is not inserted<br><br>";
    };
}


/* MAIN Process start here */
if ( $xlsx = SimpleXLSX::parse('./uploads/equip_schedule.xlsx') ) {
	mysqli_query($db,'SET FOREIGN_KEY_CHECKS = 0;');
    

    // Import all definition of codes to the database
    $key = array_search('codedefinition', $xlsx->sheetNames());

    foreach($xlsx->rows($key) as $row){
        insert_codes($xlsx, $db ,$row[0], $row[1],  $row[2]);
    }

    //  Import equip_info to the database
    $records = array('equip_info', 'change_record', 'relocation_record', 'mro_record');
    foreach ($records as $tablename){
        insert_records($xlsx, $db , $tablename);
    }
    mysqli_query($db,'SET FOREIGN_KEY_CHECKS = 1;');
} else {
	echo SimpleXLSX::parseError();
}
