const Promise = require('bluebird')
const JDBC = Promise.promisifyAll(require('jdbc'))
const jinst = require('jdbc/lib/jinst')
const path = require('path')
const _ = require('lodash')

const defaultConfig = {
  url: 'jdbc:hive2://192.168.0.223:10000/default',
  drivername: 'org.apache.hive.jdbc.HiveDriver',
  user: 'hive',
  password: '',
  minpoolsize: 1,
  maxpoolsize: 10,
  maxidle: 20*60*1000, //20 minutes
  // keepalive: {
  //   interval: 45*60*1000,
  //   query: 'select 1',
  //   enabled: true
  // },
  properties: {}
}

const mapParam = (preparedStatement) => {
  return (param, idx) => {
    const paramIndex = idx + 1
    switch(typeof param) {
      case 'string':
        return preparedStatement.setStringAsync(paramIndex, param)
      case 'number':
        return preparedStatement.setIntAsync(paramIndex, param)
      case 'boolean':
        return preparedStatement.setBooleanAsync(paramIndex, param)
      default:
        throw new Error('No mapping available for type ' + typeof param + ' value: ' + param)
      }
  }
}


/**
 * @description jdbc连接hiveServer2
 * @class HiveJdbc
 */
module.exports = class HiveJdbc {

  constructor(config) {
    this.config = Object.assign(defaultConfig, config)
    this.jdbcInstance = null
    // create a jvm and specify the jars required in the classpath and other jvm
    // parameters
    if (!jinst.isJvmCreated()) {
      jinst.addOption("-Xrs")
      jinst.setupClasspath([
        path.resolve(__dirname, '../drivers/hive-jdbc-2.1.0-standalone.jar'),
        path.resolve(__dirname, '../drivers/hadoop-common-2.7.0-mapr-1607.jar')
      ])
    }
    this.Connection = null
    this.Statement = null
  }

  /**
   * @description 初始化连接
   * @returns
   * @memberOf HiveJdbc
   */
  async connect() {
    return new Promise((resolve, reject) => {
      this.jdbcInstance = new JDBC(this.config)
      this.jdbcInstance.initialize(err => {
        if (err) reject(err)
      })
      this.jdbcInstance.reserve((err, connObj) => {
        if (err) reject(err)
        if (connObj) {
          resolve(connObj)
        }
      })
    })
  }

  /**
   * @description 获取hive连接
   * @returns 连接对象
   * @memberOf HiveJdbc
   */
  async getConnection() {
    if (!this.Connection) {
      this.Connection = await this.connect()
    }
    return this.Connection.conn
  }

  async getStatement() {
    if (!this.Statement) {
      const conn = await this.getConnection()
      this.Statement = await new Promise((resolve, reject) => {
        conn.createStatement((err, statement) => {
          if (err) reject(err)
          resolve(statement)
        })
      })
    }
    return this.Statement
  }

  /**
   * @description 执行hql语句含税
   * @param {string} sql hql语句
   * @param {Array} [params=[]] hql语句对应参数，可多个数组; 默认[]
   * @param {boolean} [isQuery=true] 是否为查询语句; 默认true
   * @returns hql执行结果
   * @memberOf HiveJdbc
   */
  async runQuery(sql, params = [], isQuery = true) {
    // console.log('executing statement. SQL:  ', sql, ' Params: ', params)
    const conn = await this.getConnection()
    const prepareAsync = Promise.promisify(conn.prepareStatement)
    const ps = await prepareAsync.call(conn, sql)
    const preparedStatement = await Promise.promisifyAll(ps)
    await Promise.all(_.map(params, mapParam(preparedStatement)))
    let result
    if(isQuery) {
      result = await preparedStatement.executeQueryAsync()
    } else {
      result = await preparedStatement.executeAsync()
    }
    if(isQuery) {
      const resultSet = Promise.promisifyAll(result)
      return resultSet.toObjArrayAsync()
    } else {
      return result
    }
  }

  async getPrepareStatement() {
    const conn = await this.getConnection()
    if (!this.PrepareStatement) {
      this.PrepareStatement = Promise.promisify(conn.prepareStatement)
    }
    return this.PrepareStatement
  }
}