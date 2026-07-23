// Copyright (c) LightPool Labs
// Author: xiaoyu1998

pub fn slug_from_question(question: &str) -> String {
    let mut slug = String::new();
    let mut last_hyphen = false;

    for c in question.trim().to_lowercase().chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
            last_hyphen = false;
        } else if !last_hyphen {
            slug.push('-');
            last_hyphen = true;
        }
    }

    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        return "event".into();
    }
    slug
}

pub fn allocate_unique_slug(existing: &[String], question: &str) -> String {
    let base = slug_from_question(question);
    let mut slug = base.clone();
    let mut suffix = 2;

    while existing.iter().any(|value| value == &slug) {
        slug = format!("{base}-{suffix}");
        suffix += 1;
    }

    slug
}
