CLI tool that decodes event logs produced by ```cryo``` into readable values that can be used in testing. Also can help in adding timestamps. 


````sh
./target/release/eth_event_decoder -a {abi}.json -l {filename}.csv / {folder with csvs} -t {timestamps y/n}
````