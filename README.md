[<picture><source media="(prefers-color-scheme: dark)" srcset="https://brokerql.github.io/images/logo/logo-auto-color.svg"><source media="(prefers-color-scheme: light)" srcset="https://brokerql.github.io/images/logo/logo-auto-color.svg"><img width="67%" alt="BrokerQL Logo" src="https://brokerql.github.io/images/logo/logo-auto-color.svg"></picture>](https://brokerql.github.io?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme)

BrokerQL is the database interface to Brokers and Finance Market. Use SQL to query quotes, positions, query or place orders, and find investing or trading ideas.

BrokerQL is built with ❤ by trader, and built for traders, portfolio managers, and financial advisors who knows a little about SQL, to decrease daily job of portfolio management.

BrokerQL is inspired by [Steampipe](https://steampipe.io/).

With [BrokerQL](https://brokerql.github.io?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme) you can:

- **Query** → Use SQL to query (and join across!) from your Brokers.

- **Modify** → Update multiple order's price and quantity with SQL.  


## BrokerQL CLI: The SQL console for Online Brokers.

The BrokerQL community has grown a suite of plugins that map Brokers to tables.

<table>
  <tr>
   <td><b>US Brokers</b></td>
   <td><a href="https://www.interactivebrokers.com/">IBKR</a> …</td>
  </tr>
</table>



The interactive query shell is one way you can query those tables.

<img width="524" src="https://brokerql.github.io/assets/demo.gif" />

You can also use mysql-client, mycli, DataGrip, or any client that can connect to MySQL.

### Get started with the CLI

<details>

 <summary>Install BrokerQL</summary>
 <br/>

The <a href="https://brokerql.github.io/download?utm_id=gspreadme&utm_source=github&utm_medium=repo&utm_campaign=github&utm_content=readme">download</a> page shows you how but tl;dr:

Linux or Windows or macOS

```sh
pip install BrokerQL
```

macOS only

```sh
brew tap BrokerQL/tap
brew install broker-ql
```

</details>


 <details>
 <summary>Run <tt>BrokerQL query</tt></summary>
<br/>

Make sure your IBKR TWS is running on localhost, and listen to port 7496 for API.

<img width="524" src="https://brokerql.github.io/images/doc/tws_api_config.png" />

Launch the interactive shell.

```sh
broker-ql --cli
```

Run your first query!

```sql
select * from tws.positions;
```
</details>

<details>
 <summary>Learn more about the CLI</summary>

- It's just SQL

- You can run queries on the command line and include them in scripts.

 </details>

 <details>
 <summary>Build and develop the CLI</summary>

Prerequisites:

- Python Version 3.9.

Clone:

```sh
git clone git@github.com:BrokerQL/BrokerQL
cd BrokerQL
```

Create virtualenv, and run the pip command

```
python -m venv venv
source venv/bin/activate
pip install .
pip install -r requirements-dev.txt
```

Check the version

```
$ broker-ql -v
BrokerQL v0.0.1
```

Try it!

```
broker-ql --cli
connecting to tws...
tws connected
BrokerQL (none)> nopager
Pager disabled.
Time: 0.000s
BrokerQL (none)> show databases
+--------------------+
| Database           |
+--------------------+
| information_schema |
| mysql              |
| tws                |
+--------------------+
3 rows in set
Time: 0.036s
BrokerQL (none)> use tws
You are now connected to database "tws" as user "panjiabang"
Time: 0.001s
BrokerQL tws> show tables
+---------------+
| Table_name    |
+---------------+
| orders        |
| positions     |
| quotes        |
| subscriptions |
+---------------+
4 rows in set
Time: 0.033s
BrokerQL tws> select * from positions
+-----------+--------+----------+--------------------+
| account   | symbol | position | avg_cost           |
+-----------+--------+----------+--------------------+
| U11739578 | ADBE   |  6.0     | 579.3533333333334  |
| U11739578 | PDD    | 32.0     | 119.65125          |
| U11739578 | URNM   | 62.0     |  49.18850806451613 |
| U11739578 | QQQ    | 27.0     | 385.82703703703703 |
| U11739578 | MSFT   | 12.0     | 378.9533333333333  |
| U11739578 | MHO    | 40.0     | 103.19500000000001 |
+-----------+--------+----------+--------------------+
12 rows in set
Time: 0.037s
```
</details>

## Community

We thrive on feedback and community involvement!

**Have a question?** →  Join our [Github Discussions](https://github.com/BrokerQL/BrokerQL/discussions) or open a [GitHub issue](https://github.com/BrokerQL/BrokerQL/issues/new/choose).
