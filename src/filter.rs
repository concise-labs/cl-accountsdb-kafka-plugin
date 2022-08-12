// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate curl;
use curl::easy::Easy;

use {
    crate::*,
    log::info,
    solana_program::pubkey::Pubkey,
    std::{collections::HashSet, str::FromStr, str},
    std::time::{Duration, Instant},
    serde::Deserialize,
};

#[derive(Deserialize)]
struct S3Program {
    program: Vec<String>,
}

impl S3Program {
    pub fn new() -> Self {
        let mut default_vec = Vec::new();
        default_vec.push("bigb".to_string());
        Self {
            program: default_vec,
        }
    }
}

pub struct Filter {
    program_ignores: HashSet<[u8; 32]>,
    program_whitelist: HashSet<[u8; 32]>,
    last_updated_ts: Instant,
}

impl Filter {
    pub fn new(config: &Config) -> Self {
        Self {
            program_ignores: config
                .program_ignores
                .iter()
                .flat_map(|p| Pubkey::from_str(p).ok().map(|p| p.to_bytes()))
                .collect(),
            program_whitelist: config
                .program_whitelist
                .iter()
                .flat_map(|p| Pubkey::from_str(p).ok().map(|p| p.to_bytes()))
                .collect(),
            last_updated_ts: Instant::now(),
        }
    }

    pub fn update_program_whitelist(&mut self){
        let mut raw_out: Vec<u8> = Vec::new();

        // change program whitelist to add current timestamp
        if self.last_updated_ts.elapsed() >= Duration::from_secs(3600) { // every 1 hour
            self.last_updated_ts = Instant::now();
            info!("conciselabs updating because 5 secs happened updated ts: {:?} ", self.last_updated_ts);

            self.get_data_from_s3(&mut raw_out);
            let unmut_raw_out = raw_out;
            let s = match str::from_utf8(&unmut_raw_out) {
                Ok(v) => v.to_string(),
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
    
            let program_whitelist_from_s3: S3Program = serde_json::from_str(&s).unwrap_or(S3Program::new());
            info!("conciselabs json from S3 {:#?}", program_whitelist_from_s3.program);
            self.program_whitelist =  program_whitelist_from_s3.program
                                    .iter()
                                    .flat_map(|p| Pubkey::from_str(p).ok().map(|p| p.to_bytes()))
                                    .collect();
        }
    }


    pub fn get_data_from_s3(&mut self, raw_out: &mut Vec<u8>){
        let mut easy = Easy::new();

        easy.url("https://cl-accounts-wl.s3.amazonaws.com/cl-accounts-wl.json").unwrap();
        let mut transfer = easy.transfer();

        transfer.write_function(|data| {
            raw_out.extend_from_slice(data);
            Ok(data.len())
        }).unwrap();            
        transfer.perform().unwrap();

    }

    pub fn wants_program(&self, program: &[u8]) -> bool {
        info!("conciselabs: inside wants_program: program_whitelist: {:#?}", self.program_whitelist);
        let key = match <&[u8; 32]>::try_from(program) {
            Ok(key) => key,
            _ => return true,
        };
        info!("conciselabs: inside wants_program: key after match: {}", Pubkey::new(key));
        !self.program_ignores.contains(key) && self.program_whitelist.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter() {
        let config = Config {
            program_ignores: vec![
                "Sysvar1111111111111111111111111111111111111".to_owned(),
                "Vote111111111111111111111111111111111111111".to_owned(),
            ],
            ..Config::default()
        };

        let filter = Filter::new(&config);
        assert_eq!(filter.program_ignores.len(), 2);

        assert!(filter.wants_program(
            &Pubkey::from_str("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin")
                .unwrap()
                .to_bytes()
        ));
        assert!(!filter.wants_program(
            &Pubkey::from_str("Vote111111111111111111111111111111111111111")
                .unwrap()
                .to_bytes()
        ));
    }
}
