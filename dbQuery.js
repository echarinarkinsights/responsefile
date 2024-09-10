const mysql = require('mysql2');

// Database configuration
const dbConfig = {
  host: 'localhost',
  user: 'root', // Replace with your MySQL username
  password: '', // Replace with your MySQL password
  database: 'icx_sg' // Replace with your database name
};

/**
 * Query the database
 * @param {string} query - The SQL query to execute
 * @param {Array} [params=[]] - The parameters for the query
 * @returns {Promise<Array>} - A promise that resolves with the query results
 */
function queryDatabase(query, params = []) {
  return new Promise((resolve, reject) => {
    // Create a connection to the database
    const connection = mysql.createConnection(dbConfig);

    // Connect to the database
    connection.connect((err) => {
      if (err) {
        return reject('Error connecting to database: ' + err.stack);
      }

      // Execute the query
      connection.query(query, params, (err, results) => {
        // Close the connection
        connection.end();

        if (err) {
          return reject('Error executing query: ' + err.stack);
        }

        // Resolve with the results
        resolve(results);
      });
    });
  });
}

// Export the function for use in other files
module.exports = queryDatabase;
