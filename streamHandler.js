var AWS = require("aws-sdk");

// https://github.com/jeremydaly/serverless-mysql
const mysql = require('serverless-mysql')({
    config: {
      host     : process.env.AURORA_HOST,
      database : process.env.AURORA_DB_NAME,
      user     : process.env.AURORA_USERNAME,
      password : process.env.AURORA_PASSWORD,
      port     : process.env.AURORA_PORT
    }
})
  
exports.handler = async (event, context) => {
    console.log(event)
    const records = await event.Records;
    console.log(records)
    for (record in records) { 
        console.log('Stream record: ', JSON.stringify(records[record], null, 2));
        if (records[record].eventName == 'INSERT' || records[record].eventName == 'MODIFY') {
            var newRecord = records[record].dynamodb.NewImage;
            console.log(JSON.stringify(newRecord));
            let results = await mysql.transaction()
                .query(`
                    CREATE TABLE IF NOT EXISTS songs (
                        song varchar(60) NOT NULL PRIMARY KEY,
                        votes int NOT NULL
                    );`)
                .query(`
                INSERT INTO songs (song, votes)
                VALUES ("${newRecord.songName.S}", 1)
                ON DUPLICATE KEY UPDATE votes = votes + 1;
                `)
                .rollback(e => { console.log("Error from SQL:" + e) })
                .commit() // execute the queries
            await mysql.end()
            return results
        }
    }
}