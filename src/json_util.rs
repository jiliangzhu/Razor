pub fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().ok();
    }
    v.as_f64()
}
