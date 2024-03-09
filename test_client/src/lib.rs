use std::{collections::HashMap, vec};

// 一个简单的词频统计(only英文)的案例.

#[no_mangle]
pub fn mapper(content : &String) -> HashMap<String, Vec<String>> {
    let mut ret:HashMap<String, u32> = HashMap::new();
    for str in content.split(' ') {
        // 如果str中每个都是字母，就当成一个单词...
        if str.is_empty() {
            continue;
        }
        let mut is_word = true;   // 如果全是英文字母就当做是单词..
        for c in str.chars() {
            if !c.is_ascii_alphabetic(){
                is_word = false;
                break;
            }
        }
        if !is_word {
            continue;
        }
        let word = str.to_string().to_lowercase();
        match ret.get_mut(&word) {
            Some(v) => {
                *v += 1;
            },
            None => {
                ret.insert(word, 1);
            }
        };
    }

    let ret:HashMap<String,Vec<String>> = ret.into_iter()
                .map(|(k,v)| (k,vec![v.to_string()]))
                .collect();
    ret
}

#[no_mangle]
pub fn reducer(key:&String, value: &Vec<String>) -> Vec<String> {
    let mut ret:Vec<String> = Vec::new();
    let mut sum = 0;
    for num in value {
        sum += num.parse::<u32>().unwrap();  // 不应有错..
    }
    ret.push(sum.to_string());
    ret
}