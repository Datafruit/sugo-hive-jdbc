const HiveJdbc = require('../src/index')

const conf = {
  url: 'jdbc:hive2://192.168.0.223:10000/default',
  drivername: 'org.apache.hive.jdbc.HiveDriver',
  minpoolsize: 10,
  maxpoolsize: 100,
  properties: {
    user: 'hive',
    password: ''
  }
}

// (async () => {

// })()

const main = async () => {
  const hive = new HiveJdbc(conf)
  // const conn = await hive.getConnection()
  // const res = await hiveKit.runQuery('select * from users where userid=? limit 10', ['c81e728d9d4c2f636f067f89cc14862c'])
  const res = await hive.runQuery('select count(*) from users')
  console.log(res)
}

main()