use std::cmp::Ordering;
use std::collections::HashSet;
use std::collections::VecDeque;

use chrono::DateTime;

#[derive(Debug, Clone)]
pub struct Item<T, U> {
  data: T,
  key: U,
}

pub fn parse_logs(lines: VecDeque<String>) -> VecDeque<crate::Item<String, u64>> {
  let mut logs: VecDeque<crate::Item<String, u64>> = VecDeque::new();

  for line in lines {
    if !line.is_empty() {
      let split_log = line.split(" ").collect::<Vec<_>>();
      let item = crate::Item {
        data: line.clone(),
        key: DateTime::parse_from_str(
          &format!("{} {}", split_log[3], split_log[4]),
          &"[%d/%b/%Y:%H:%M:%S %z]",
        )
        .unwrap()
        .timestamp() as u64,
      };
      logs.push_back(item);
    }
  }

  logs
}

pub fn merge(mut logs: Vec<VecDeque<crate::Item<String, u64>>>) -> VecDeque<String> {
  let mut result: VecDeque<String> = VecDeque::new();

  loop {
    let mut min: u64 = std::u64::MAX;

    // Assume all logs are non-empty
    let mut non_empty_logs: HashSet<usize> = (0..logs.len()).collect();

    // Assume the first log has the earliest timestamp
    let mut min_index: usize = 0;

    // Find the actual log with the earliest timestamp
    for i in non_empty_logs.clone().iter() {
      if !logs[*i].is_empty() {
        if logs[*i].get(0).unwrap().key < min {
          min = logs[*i].get(0).unwrap().key;
          min_index = *i;
        }
      } else {
        non_empty_logs.remove(&i); // correct if assumption is false
      }
    }

    if !logs[min_index].is_empty() {
      let line = logs[min_index].pop_front().unwrap().data;
      result.push_back(line);
    } else {
      break;
    }
  }

  result
}

impl PartialOrd for Item<String, u64> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    if self.key < other.key {
      Some(Ordering::Less)
    } else if self.key > other.key {
      Some(Ordering::Greater)
    } else {
      Some(Ordering::Equal)
    }
  }
}

impl Ord for Item<String, u64> {
  fn cmp(&self, other: &Self) -> Ordering {
    if self.key < other.key {
      Ordering::Less
    } else if self.key > other.key {
      Ordering::Greater
    } else {
      Ordering::Equal
    }
  }
}

impl PartialEq for Item<String, u64> {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key
  }
}

impl Eq for Item<String, u64> {}

#[cfg(test)]
mod tests {
  use chrono::DateTime;
  use std::collections::VecDeque;
  use std::fs::File;
  use std::io::{self, BufRead};
  use std::path::Path;

  #[test]
  fn it_works() {
    let mut data = String::from("92.254.10.150 - - [01/Jul/2022:10:53:49 +0000] \"POST /wp-content/tag/explore HTTP/2\" 500 434 \"-\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_10_4; rv:1.9.5.20) Gecko/2971-11-13 11:20:46 Firefox/11.0\"");

    let mut timestamp: u64 =
      DateTime::parse_from_str("[01/Jul/2022:10:53:49 +0000]", "[%d/%b/%Y:%H:%M:%S %z]")
        .unwrap()
        .timestamp() as u64;

    let log_line1 = crate::Item {
      data: data.clone(),
      key: timestamp,
    };

    data = String::from("92.254.10.150 - - [02/Jul/2022:12:53:49 +0000] \"POST /wp-content/tag/explore HTTP/2\" 500 434 \"-\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_10_4; rv:1.9.5.20) Gecko/2971-11-13 11:20:46 Firefox/11.0\"");
    timestamp = DateTime::parse_from_str("[02/Jul/2022:12:53:49 +0000]", "[%d/%b/%Y:%H:%M:%S %z]")
      .unwrap()
      .timestamp() as u64;

    let log_line2 = crate::Item {
      data: data.clone(),
      key: timestamp,
    };

    println!("{:#?}", log_line1.cmp(&log_line2));
  }

  fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
  where
    P: AsRef<Path>,
  {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
  }

  #[test]
  fn read_files() {
    let mut logs_from_hosts: Vec<VecDeque<crate::Item<String, u64>>> = vec![
      VecDeque::new(),
      VecDeque::new(),
      VecDeque::new(),
      VecDeque::new(),
    ];
    for i in 1..5 {
      if let Ok(lines) = read_lines(format!("./tests/{}.log", i)) {
        for line in lines {
          if let Ok(logline) = line {
            let split_log = logline.split(" ").collect::<Vec<_>>();
            let item = crate::Item {
              data: logline.clone(),
              key: DateTime::parse_from_str(
                &format!("{} {}", split_log[3], split_log[4]),
                &"[%d/%b/%Y:%H:%M:%S %z]",
              )
              .unwrap()
              .timestamp() as u64,
            };
            logs_from_hosts[i - 1].push_back(item);
          }
        }
      }
    }

    println!("{:#?}", crate::merge(logs_from_hosts));
  }
}
