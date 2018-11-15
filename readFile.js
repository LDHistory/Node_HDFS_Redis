var redis = require('redis'),
    client = redis.createClient();

var async = require('async');

var data = [];
var csvArray = new Array();

var task = [
    function (callback){
        var webHDFS = require('webhdfs');
        var hdfs = webHDFS.createClient({
            user: 'hadoop',
            host: 'hadoop',
            port: 50070,
            path: '/webhdfs/v1'
        });

        console.time('getData');

        var remoteFileStream  = hdfs.createReadStream('/user/hadoop/test.csv');

        remoteFileStream.on('error', function (err){
            //Do something
        });

        remoteFileStream.on('data', function (chunk){
            //Do something
            data.push(chunk);
        });

        remoteFileStream.on('finish', function (){
            //Do something
            console.timeEnd('getData');
            callback(null, "Successfully Read a Data");
        });
    },
    function (callback){
        console.time('insertData');
        var commandList = [];
        
        // var aJsonArray = JSON.parse(Buffer.concat(data).toString());
        var csvBuffer = Buffer.concat(data).toString();
        var cArray = csvBuffer.split("\n");
        for (let i = 1, x = cArray.length; i < x; i++){
            var element = cArray[i].split(",");
            commandList.push(['zadd', element[0],
             element[1], element[2]]);
        }

        client.multi(commandList).exec((err, replies) =>{
            if (err){
                console.log(err);
            }else if (replies == 0){
                console.log(replies);
            }
        });
        // for (let i = 0, x = aJsonArray.length; i < x; i++){
        //     commandList.push(['zadd', aJsonArray[i].key,
        //      aJsonArray[i].score, aJsonArray[i].value]);
        // }
        
        // client.multi(commandList).exec((err, replies) =>{
        //     if (err){
        //         console.log(err);
        //     }else if (replies == 0){
        //         console.log(replies);
        //     }
        // });

        console.timeEnd('insertData');
        callback(null, "Successfully process a data");
    }
];

async.series(task, function (err, results){
    console.log(results);
});