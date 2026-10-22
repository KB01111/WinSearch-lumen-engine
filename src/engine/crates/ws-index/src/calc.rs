//! Arithmetic evaluator for calculator-style queries.
//!
//! A small recursive-descent parser over f64 with the usual operators,
//! parentheses, common functions and constants. This is deliberately *not* a
//! general scripting language: only arithmetic syntax is accepted, nothing is
//! executed, and a query only produces a calculator result when it parses as a
//! complete expression containing at least one operation.

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CalcError {
    #[error("unexpected character '{0}'")]
    UnexpectedChar(char),
    #[error("unexpected end of expression")]
    UnexpectedEnd,
    #[error("expected {0}")]
    Expected(&'static str),
    #[error("unknown name '{0}'")]
    UnknownName(String),
    #[error("unknown function '{0}'")]
    UnknownFunction(String),
    #[error("wrong number of arguments for '{0}'")]
    Arity(String),
    #[error("division by zero")]
    DivisionByZero,
    #[error("result is not a finite number")]
    NotFinite,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CalcResult {
    /// The expression as written (trimmed).
    pub expression: String,
    /// Human-readable result (integer without decimals when exact).
    pub value_text: String,
    pub value: f64,
}

/// Evaluates `input` if it is a complete arithmetic expression; returns
/// `Ok(None)` when the text is not expression-like (ordinary search text).
///
/// The not-a-calculation decision is made up front: every word in the input
/// must be a known function or constant. That way queries like `report-2.pdf`
/// are ordinary search text, while `2 + 2`, `sqrt(16)`, `ln(e)` and `1 / 0`
/// are treated as calculations (the last one failing with a real error rather
/// than silently matching nothing).
pub fn evaluate(input: &str) -> Result<Option<CalcResult>, CalcError> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.len() > 256 {
        return Ok(None);
    }
    if !only_math_words(trimmed) {
        return Ok(None);
    }

    // A lexing failure means non-math characters are present.
    let tokens = match tokenize(trimmed) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    let mut parser = Parser {
        tokens: &tokens,
        pos: 0,
        operated: false,
    };
    let value = parser.parse_expr()?;
    if parser.pos != parser.tokens.len() {
        return Ok(None); // trailing junk: ordinary search text
    }
    if !parser.operated {
        return Ok(None); // a bare number is not a calculation
    }
    let value = if value.is_finite() {
        value
    } else {
        return Err(CalcError::NotFinite);
    };
    Ok(Some(CalcResult {
        expression: trimmed.to_string(),
        value_text: format_value(value),
        value,
    }))
}

/// Formats a result: integers without a fractional part print plainly, other
/// values print with up to 10 decimals and trailing zeros trimmed.
fn format_value(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    if v.fract() == 0.0 && v.abs() < 1e15 {
        return format!("{}", v as i64);
    }
    if v.abs() >= 1e15 || v.abs() < 1e-9 {
        return format!("{v:e}");
    }
    let s = format!("{v:.10}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    s.to_string()
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Ident(String),
    Op(char),
    LParen,
    RParen,
    Comma,
}

fn tokenize(input: &str) -> Result<Vec<Token>, CalcError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '0'..='9' | '.' => {
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '.' || c == 'e' || c == 'E' {
                        // 'e' is only a valid exponent marker when followed by
                        // digits or a sign; otherwise leave it to identifiers.
                        if c == 'e' || c == 'E' {
                            let mut lookahead = chars.clone();
                            lookahead.next();
                            match lookahead.peek() {
                                Some('+') | Some('-') => {
                                    lookahead.next();
                                    if !lookahead.peek().is_some_and(|c| c.is_ascii_digit()) {
                                        break;
                                    }
                                }
                                Some(c2) if c2.is_ascii_digit() => {}
                                _ => break,
                            }
                        }
                        num.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let value: f64 = num
                    .parse()
                    .map_err(|_| CalcError::UnexpectedChar(num.chars().next().unwrap_or('?')))?;
                tokens.push(Token::Number(value));
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut ident = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_alphanumeric() || c == '_' {
                        ident.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Ident(ident));
            }
            '+' | '-' | '*' | '/' | '%' | '^' => {
                tokens.push(Token::Op(c));
                chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
            }
            other => return Err(CalcError::UnexpectedChar(other)),
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    /// True once the expression contained an operator or function call.
    operated: bool,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<&'a Token> {
        let t = self.tokens.get(self.pos);
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn parse_expr(&mut self) -> Result<f64, CalcError> {
        let mut value = self.parse_term()?;
        loop {
            match self.peek() {
                Some(Token::Op('+')) => {
                    self.pos += 1;
                    self.operated = true;
                    value += self.parse_term()?;
                }
                Some(Token::Op('-')) => {
                    self.pos += 1;
                    self.operated = true;
                    value -= self.parse_term()?;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_term(&mut self) -> Result<f64, CalcError> {
        let mut value = self.parse_unary()?;
        loop {
            match self.peek() {
                Some(Token::Op('*')) => {
                    self.pos += 1;
                    self.operated = true;
                    value *= self.parse_unary()?;
                }
                Some(Token::Op('/')) => {
                    self.pos += 1;
                    self.operated = true;
                    let rhs = self.parse_unary()?;
                    if rhs == 0.0 {
                        return Err(CalcError::DivisionByZero);
                    }
                    value /= rhs;
                }
                Some(Token::Op('%')) => {
                    self.pos += 1;
                    self.operated = true;
                    let rhs = self.parse_unary()?;
                    if rhs == 0.0 {
                        return Err(CalcError::DivisionByZero);
                    }
                    value %= rhs;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_unary(&mut self) -> Result<f64, CalcError> {
        match self.peek() {
            Some(Token::Op('-')) => {
                self.pos += 1;
                self.operated = true;
                Ok(-self.parse_unary()?)
            }
            Some(Token::Op('+')) => {
                self.pos += 1;
                self.parse_unary()
            }
            _ => self.parse_power(),
        }
    }

    fn parse_power(&mut self) -> Result<f64, CalcError> {
        let base = self.parse_primary()?;
        if let Some(Token::Op('^')) = self.peek() {
            self.pos += 1;
            self.operated = true;
            // Right-associative: 2^3^2 == 2^(3^2).
            let exp = self.parse_unary()?;
            return Ok(base.powf(exp));
        }
        Ok(base)
    }

    fn parse_primary(&mut self) -> Result<f64, CalcError> {
        match self.next() {
            Some(Token::Number(v)) => Ok(*v),
            Some(Token::LParen) => {
                let v = self.parse_expr()?;
                match self.next() {
                    Some(Token::RParen) => Ok(v),
                    _ => Err(CalcError::Expected("')'")),
                }
            }
            Some(Token::Ident(name)) => self.parse_ident(name),
            Some(Token::Op(c)) => Err(CalcError::UnexpectedChar(*c)),
            Some(Token::RParen) | Some(Token::Comma) => Err(CalcError::Expected("a number")),
            None => Err(CalcError::UnexpectedEnd),
        }
    }

    fn parse_ident(&mut self, name: &str) -> Result<f64, CalcError> {
        let lower = name.to_ascii_lowercase();
        if matches!(self.peek(), Some(Token::LParen)) {
            self.pos += 1;
            self.operated = true;
            let mut args = Vec::new();
            if !matches!(self.peek(), Some(Token::RParen)) {
                loop {
                    args.push(self.parse_expr()?);
                    match self.peek() {
                        Some(Token::Comma) => {
                            self.pos += 1;
                        }
                        _ => break,
                    }
                }
            }
            match self.next() {
                Some(Token::RParen) => {}
                _ => return Err(CalcError::Expected("')'")),
            }
            call_function(&lower, &args)
        } else {
            match lower.as_str() {
                "pi" | "π" => Ok(std::f64::consts::PI),
                "e" => Ok(std::f64::consts::E),
                "tau" => Ok(std::f64::consts::TAU),
                _ => Err(CalcError::UnknownName(name.to_string())),
            }
        }
    }
}

fn call_function(name: &str, args: &[f64]) -> Result<f64, CalcError> {
    let one = |f: fn(f64) -> f64| match args {
        [x] => Ok(f(*x)),
        _ => Err(CalcError::Arity(name.to_string())),
    };
    let two = |f: fn(f64, f64) -> f64| match args {
        [x, y] => Ok(f(*x, *y)),
        _ => Err(CalcError::Arity(name.to_string())),
    };
    match name {
        "sqrt" => one(f64::sqrt),
        "cbrt" => one(f64::cbrt),
        "abs" => one(f64::abs),
        "round" => one(f64::round),
        "floor" => one(f64::floor),
        "ceil" => one(f64::ceil),
        "sin" => one(f64::sin),
        "cos" => one(f64::cos),
        "tan" => one(f64::tan),
        "asin" => one(f64::asin),
        "acos" => one(f64::acos),
        "atan" => one(f64::atan),
        "ln" => one(f64::ln),
        "log2" => one(f64::log2),
        "log10" | "log" => one(f64::log10),
        "exp" => one(f64::exp),
        "pow" => two(f64::powf),
        "atan2" => two(f64::atan2),
        "min" => {
            let v = args.iter().copied().fold(f64::INFINITY, f64::min);
            args.is_empty()
                .then(|| Err(CalcError::Arity(name.to_string())))
                .unwrap_or(Ok(v))
        }
        "max" => {
            let v = args.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            args.is_empty()
                .then(|| Err(CalcError::Arity(name.to_string())))
                .unwrap_or(Ok(v))
        }
        _ => Err(CalcError::UnknownFunction(name.to_string())),
    }
}

/// True when every alphabetic run in `input` is a known function or constant
/// name — i.e. the text is plausibly math rather than a filename or phrase.
fn only_math_words(input: &str) -> bool {
    input
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|w| !w.is_empty())
        .all(|w| {
            let w = w.to_ascii_lowercase();
            matches!(
                w.as_str(),
                "sqrt"
                    | "cbrt"
                    | "abs"
                    | "round"
                    | "floor"
                    | "ceil"
                    | "sin"
                    | "cos"
                    | "tan"
                    | "asin"
                    | "acos"
                    | "atan"
                    | "atan2"
                    | "ln"
                    | "log"
                    | "log2"
                    | "log10"
                    | "exp"
                    | "pow"
                    | "min"
                    | "max"
                    | "pi"
                    | "e"
                    | "tau"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(input: &str) -> f64 {
        evaluate(input)
            .unwrap_or_else(|e| panic!("{input}: {e}"))
            .unwrap_or_else(|| panic!("{input}: not expression-like"))
            .value
    }

    fn text(input: &str) -> String {
        evaluate(input).unwrap().unwrap().value_text
    }

    #[test]
    fn precedence_and_parentheses() {
        assert_eq!(value("2 + 3 * 4"), 14.0);
        assert_eq!(value("(2 + 3) * 4"), 20.0);
        assert_eq!(value("10 - 2 - 3"), 5.0);
        assert_eq!(value("100 / 10 / 2"), 5.0);
    }

    #[test]
    fn power_is_right_associative() {
        assert_eq!(value("2 ^ 3 ^ 2"), 512.0);
        assert_eq!(value("2 ^ (3 ^ 2)"), 512.0);
        assert_eq!(value("(2 ^ 3) ^ 2"), 64.0);
    }

    #[test]
    fn unary_and_modulo() {
        assert_eq!(value("-5 + 8"), 3.0);
        assert_eq!(value("-(2 + 3)"), -5.0);
        assert_eq!(value("7 % 3"), 1.0);
    }

    #[test]
    fn functions_and_constants() {
        assert_eq!(value("sqrt(16)"), 4.0);
        assert!((value("sin(pi / 2)") - 1.0).abs() < 1e-12);
        assert!((value("ln(e)") - 1.0).abs() < 1e-12);
        assert_eq!(value("round(2.6)"), 3.0);
        assert_eq!(value("min(3, 1, 2)"), 1.0);
        assert_eq!(value("max(3, 1, 2)"), 3.0);
        assert_eq!(value("pow(2, 10)"), 1024.0);
    }

    #[test]
    fn formatting() {
        assert_eq!(text("1 / 4"), "0.25");
        assert_eq!(text("2 * 21"), "42");
        assert_eq!(text("0.1 + 0.2"), "0.3");
    }

    #[test]
    fn plain_text_is_not_a_calculation() {
        assert_eq!(evaluate("hello world").unwrap(), None);
        assert_eq!(evaluate("quarterly report").unwrap(), None);
        // A bare number is a search term, not a calculation.
        assert_eq!(evaluate("42").unwrap(), None);
        assert_eq!(evaluate("").unwrap(), None);
    }

    #[test]
    fn trailing_junk_is_not_a_calculation() {
        assert_eq!(evaluate("2 + 2 x").unwrap(), None);
    }

    #[test]
    fn errors() {
        assert_eq!(evaluate("1 / 0"), Err(CalcError::DivisionByZero));
        assert_eq!(evaluate("2 +").unwrap_err(), CalcError::UnexpectedEnd);
        assert_eq!(evaluate("(1 + 2").unwrap_err(), CalcError::Expected("')'"));
        assert_eq!(evaluate("1e999 * 2").unwrap_err(), CalcError::NotFinite);
    }

    #[test]
    fn filenames_and_unknown_words_are_not_calculations() {
        assert_eq!(evaluate("model.safetensors").unwrap(), None);
        assert_eq!(evaluate("report-2.pdf").unwrap(), None);
        assert_eq!(evaluate("foo(1)").unwrap(), None);
        assert_eq!(evaluate("2 + 2 x").unwrap(), None);
    }
}
