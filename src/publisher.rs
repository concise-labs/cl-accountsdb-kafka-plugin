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

use {
    crate::*,
    log::info,
    prost::Message,
    rdkafka::{
        error::KafkaError,
        producer::{BaseRecord, Producer as KafkaProducer},
    },
    std::time::Duration,
    solana_program::pubkey::Pubkey,
};

pub struct Publisher {
    producer: Producer,
    shutdown_timeout: Duration,

    update_account_topic: String,
    slot_status_topic: String,
    transaction_topic: String,
}

impl Publisher {
    pub fn new(producer: Producer, config: &Config) -> Self {
        Self {
            producer,
            shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
            update_account_topic: config.update_account_topic.clone(),
            slot_status_topic: config.slot_status_topic.clone(),
            transaction_topic: config.transaction_topic.clone(),
        }
    }

    pub fn update_account(&self, ev: UpdateAccountEvent) -> Result<(), KafkaError> {
        let account_pubkey = Pubkey::new(&ev.pubkey);
        let program_pubkey = Pubkey::new(&ev.owner);
        info!("conciselabs: update_account of account: {}", account_pubkey);
        info!("conciselabs: update_account of program: {}", program_pubkey);
        let buf = ev.encode_to_vec();
        let record = BaseRecord::<Vec<u8>, _>::to(&self.update_account_topic)
            .key(&ev.pubkey)
            .payload(&buf);
        self.producer.send(record).map(|_| ()).map_err(|(e, _)| e)
    }

    pub fn update_slot_status(&self, ev: SlotStatusEvent) -> Result<(), KafkaError> {
        let buf = ev.encode_to_vec();
        let record = BaseRecord::<(), _>::to(&self.slot_status_topic).payload(&buf);
        self.producer.send(record).map(|_| ()).map_err(|(e, _)| e)
    }

    pub fn update_transaction(&self, ev: TransactionEvent) -> Result<(), KafkaError> {
        let payload = ev.transaction.as_ref().unwrap().message.as_ref().unwrap().message_payload.as_ref().unwrap();
        match payload {
            sanitized_message::MessagePayload::Legacy(legacy_message) => {
                let accounts = &legacy_message.account_keys;
                let program_index = legacy_message.instructions[0].program_id_index as usize;
                
                for account in accounts.iter() {
                    info!("conciselabs: update_transaction: account_keys: account: {}", Pubkey::new(account));
                }
                info!("conciselabs: update_transaction: program_id_index: {}", program_index);
                info!("conciselabs: update_transaction: program: {}", Pubkey::new(&accounts[program_index]));
            }
            sanitized_message::MessagePayload::V0(v0_loaded_message) => {
                let accounts = &v0_loaded_message.message.as_ref().unwrap().account_keys;
                let program_index = v0_loaded_message.message.as_ref().unwrap().instructions[0].program_id_index as usize;
                
                for account in accounts.iter() {
                    info!("conciselabs: update_transaction: account_keys: account: {}", Pubkey::new(account));
                }
                info!("conciselabs: update_transaction: program_id_index: {}", program_index);
                info!("conciselabs: update_transaction: program: {}", Pubkey::new(&accounts[program_index]));
            }
        }

        let buf = ev.encode_to_vec();
        let record = BaseRecord::<(), _>::to(&self.transaction_topic).payload(&buf);
        self.producer.send(record).map(|_| ()).map_err(|(e, _)| e)
    }

    pub fn wants_update_account(&self) -> bool {
        info!("conciselabs: wants_update_account: {}", !self.update_account_topic.is_empty());
        !self.update_account_topic.is_empty()
    }

    pub fn wants_slot_status(&self) -> bool {
        info!("conciselabs: wants_slot_status: {}", !self.slot_status_topic.is_empty());
        !self.slot_status_topic.is_empty()
    }

    pub fn wants_transaction(&self) -> bool {
        info!("conciselabs: wants_transaction: {}", !self.transaction_topic.is_empty());
        !self.transaction_topic.is_empty()
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.producer.flush(self.shutdown_timeout);
    }
}
