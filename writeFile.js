var redis = require("redis"),
    client = redis.createClient();
var async = require('async');
var os = require('os');

var key ="d:air:p:air:Q30:Q99:Q16552:1";

const records = [];
const output = [];

const fs = require('fs');

client.on("error", function (err){
    console.log("Error " + err);
});

var task = [
    function (callback){
        var aCsvArray = [];
        console.time('getData');

        aCsvArray.push({KEY: "KEY", VALUE: "VALUE", SCORE: "SCORE"});

        client.zrange(key, 0, 100000, 'WITHSCORES', function (err, value){
            for (let i = 0, x = value.length; i < x; i+=2) {
                aCsvArray.push({
                    KEY: key,
                    VALUE: value[i],
                    SCORE: value[i+1]
                });
            }
            
            // var sJson = JSON.stringify(aJsonArray);

            aCsvArray.forEach((d) => {
                const row = [];
                row.push(d.KEY);
                row.push(d.SCORE);
                row.push(d.VALUE);

                output.push(row.join());
            });

            fs.writeFileSync('test.csv', output.join(os.EOL));
            // fs.writeFileSync('test.json', sJson);
        
            client.quit();

            console.timeEnd('getData');

            callback(null, 'successfully search data');
        });
    },
    function (callback){
        console.time('transferData');

        var webHDFS = require('webhdfs');
        var hdfs = webHDFS.createClient({
            user: 'hadoop',
            host: 'localhost',
            port: 50070,
            path: '/webhdfs/v1'
        });

        var localFileStream = fs.createReadStream('./test.csv');
        var remoteFileStream = hdfs.createWriteStream('/user/hadoop/d:air:p:air:Q30:Q99:Q16552:1.csv');
        
        localFileStream.pipe(remoteFileStream);

        remoteFileStream.on('error', function (err){
            if (err) {
                console.log(err);
                callback(err);
            }
        });

        remoteFileStream.on('finish', function (){
            console.timeEnd('transferData');
            callback(null, 'successfully insert data');
        })

        fs.unlink('./test.csv', function(err) {
            if (err) throw err;
            console.log('successfully deleted');
        })
    }
]

async.series(task, function (err, results){
    console.log(results);
});