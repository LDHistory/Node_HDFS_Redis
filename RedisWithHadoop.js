/* Global Module*/
var redis = require("redis")
var webHDFS = require('webhdfs');

var fs = require('fs');
var os = require('os');

require('date-utils');

/*Global Value*/
var serverTime;
var retrievedKeys;
var keyCount;
var client;

/* 
* It can retrieves a key of redis from Redis server.
* Also redis server will be started in this code.
* It only search a history air data or history geo data.
* @return retrievedKeys - It is all of the key that is retrieved from redis as searcing d:air:*.
* @return keyCount - A number of all of key of retrievedKeys.
* @callback fnRetrieveData(retrievedKeys, keyCount).
*/
function fnRetrieveKey() {
    console.log("Start connection to Redis...");
    client = redis.createClient();

    client.on("error", function (err){
        console.log("Error " + err);

        return;
    })

    console.log("Complete connection with Redis!");

    console.log("Retriving a Redis Key...");
    retrievedKeys = [];

    client.keys("d:air:*", function(err, key){
        if (err){
            console.log(err);
        }

        if (key.length == 0){
            console.log("There isn't a key in Redis.")

            return;
        }

        for (let i = 0, x = key.length; i < x; i++){
            retrievedKeys.push(key[i]);
        }

        let time = new Date();
        serverTime = time.toFormat('YYYY-MM-DD/HH24:MI');

        keyCount = 0;

        fnRetrieveData(retrievedKeys, keyCount);
    })
}

/* 
* It can retrieves a data of a selected key from Redis server.
* If there is a timestamp in Redis server as String type,
* then Node will search a data from start time Stamp to "+inf".
* This method ensures that the values are loaded correctly.
* If both a processed key and all of key in redis is same, then next method will be run.
* @return currentKey - a selected key from list.
* @return retrievedData - a retrieved data from Redis server by currentKey.
* @callback fnRetrieveData(retrievedKeys, paramkeyCount), fnCreateCsv(retrievedData, currentKey, serverTime).
*/
function fnRetrieveData(retrievedKeys, paramkeyCount){
    let retrievedMemberCount = 0;
    let memberCount = 0;
    let start;
    let retrievedData = [];

    keyCount = paramkeyCount;
    currentKey = retrievedKeys[keyCount];

    console.log("Retriving a Redis Data...");

    client.get("string:" + currentKey, function(err, getStamp){

        v.push({KEY:"KEY", VALUE:"VALUE", SCORE:"SCORE"});

        if (getStamp == null){
            start = "-inf";
        } else{
            start = Number(getStamp) + 1;
        }

        client.zrangebyscore(currentKey, start, "+inf", 'WITHSCORES', function (err, value){
            if(err){
                console.log(err);
            }

            for (let i = 0, x = value.length; i < x; i+=2) {
                retrievedData.push({
                    KEY: currentKey,
                    VALUE: value[i],
                    SCORE: value[i+1]
                });
            }

            retrievedMemberCount = retrievedData.length - 1;

            client.zcount(currentKey, start, "+inf", function(err, number){
                if(err){
                    console.log(err);
                }
    
                memberCount = number;

                if(retrievedMemberCount != memberCount){

                    fnRetrieveData(retrievedKeys, keyCount);
                }
            });

            if (retrievedMemberCount != 0){
                client.set("string:" + currentKey, retrievedData[retrievedMemberCount].SCORE);
                
                keyCount++;
                fnCreateCsv(retrievedData, currentKey, serverTime);
            }else {
                console.log(currentKey + " is not changed.");

                if (keyCount + 1 < retrievedKeys.length){

                    keyCount++;
                    fnRetrieveData(retrievedKeys, keyCount);
                }else{
                    console.log("Process Close...");
                }
            }
        });

    });
}

/* 
* Here can create a csv file based on the retrievedData.
* Also It can define a file name. It will be used to other method.
* @return currentFileName - It finally define a file name to save a csv in local and hdfs.
* @return saveTime[0] - It contains a year, month and day data.
* @return retrievedKeys - It is all of the key that is retrieved from redis as searcing d:air:*.
* @callback fnSendCsv(currentFileName, saveTime[0], retrievedKeys).
*/
function fnCreateCsv(retrievedData, currentKey, serverTime){
    var output = [];

    saveTime = serverTime.split("/");

    retrievedData.forEach((d) => {
        const row = [];
        row.push(d.KEY);
        row.push(d.VALUE);
        row.push(d.SCORE);

        output.push(row.join());
    });

    console.log("Creating a CSV File...");

    currentKey = currentKey.replace(/:/gi, '-');

    let time =saveTime[1].split(":");

    currentFileName = currentKey + "_" + "H" + time[0] + "M" + time[1] + ".csv";

    fs.writeFileSync(currentFileName, output.join(os.EOL));

    console.log("Sucessfully create a CSV!");

    fnSendCsv(currentFileName, saveTime[0], retrievedKeys);
}

/* 
* Here can transfer a csv file to HDFS.
* It uses a webHDFS which is a based on the REST API.
* If you want to use a host as local, Change host to local.
* If you want to use a host as outside server,
* you should add a hostname and ip address into hostfile.
* HDFS Directory will be created as 2 directory that is History Data and Geo Data.
* If there is a key to be preocessed, fnRetrieveData will be run until processing all of keys.
* @callback return, fnRetrieveData(retrievedKeys, keyCount).
*/
function fnSendCsv(currentFileName, saveTime, retrievedKeys){
    let categoryPath;
    let filePath;
    let path = '/user/hadoop/';

    timeData = saveTime.split("-");

    var hdfs = webHDFS.createClient({
        user: 'hadoop',
        host: 'hadoop',
        port: 50070,
        path: '/webhdfs/v1'
    });

    console.log("Transfer a CSV File...");

    var localFileStream = fs.createReadStream('./' + currentFileName);

    fileKey = currentFileName.split("_")[0];

    if (fileKey.substring(8, 11) == "air"){
        categoryPath = "Redis_HistoryData";
    }else if(fileKey.substring(8, 11) == "geo"){
        categoryPath = "Redis_GeoData";
    }

    hdfs.exists(path + categoryPath, function(exists){
        if (!exists){
            hdfs.mkdir(path + categoryPath);
        }
        filePath = path + categoryPath;
        
        hdfs.exists(filePath + "/" + timeData[0], function(exists){
            if (!exists){
                hdfs.mkdir(filePath + "/" + timeData[0]);
            }
            filePath = filePath + "/" + timeData[0];

            hdfs.exists(filePath + "/" + timeData[1], function(exists){
                if (!exists){
                    hdfs.mkdir(filePath + "/" + timeData[1]);
                }
                filePath = filePath + "/" + timeData[1];

                hdfs.exists(filePath + "/" + timeData[2], function(exists){
                    if (!exists){
                        hdfs.mkdir(filePath + "/" + timeData[2]);
                    }
                    filePath = filePath + "/" + timeData[2];

                    var remoteFileStream = hdfs.createWriteStream(filePath + "/" + currentFileName);

                    localFileStream.pipe(remoteFileStream);

                    remoteFileStream.on('error', function (err){
                        if (err) {
                            console.log(err);
                        }
                    });
            
                    remoteFileStream.on('finish', function (){
                        console.log('transferData');

                        fs.unlink('./' + currentFileName, function(err) {
                            if (err) throw err;
                            console.log('successfully deleted');
                        })

                        if (retrievedKeys.length - 1 == keyCount){
                            client.quit();
                            hdfs.quit;

                            console.log("Processing Close...");
                            return;

                        } else {
                            fnRetrieveData(retrievedKeys, keyCount);
                        }
                    });
                });
            });
        });
    });
}

fnWatchingRedis = function(){
    setInterval(function() {
        fnRetrieveKey();
    }, 10000)
}

fnWatchingRedis();