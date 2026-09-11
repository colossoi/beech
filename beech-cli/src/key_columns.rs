use beech_core::Field;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct KeyColumns {
    columns: Vec<KeyColumn>,
}

#[derive(Debug, Clone)]
enum KeyColumn {
    Index(usize),
    Name(String),
}

impl FromStr for KeyColumns {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let columns = s
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|col| {
                // Try parsing as index first
                if let Ok(index) = col.parse::<usize>() {
                    Ok(KeyColumn::Index(index))
                } else {
                    // Treat as name
                    Ok(KeyColumn::Name(col.to_string()))
                }
            })
            .collect::<Result<Vec<KeyColumn>, anyhow::Error>>()?;

        if columns.is_empty() {
            return Err(anyhow::anyhow!("At least one key column must be specified"));
        }

        Ok(KeyColumns { columns })
    }
}

impl KeyColumns {
    pub fn to_indices(&self, fields: &[Field]) -> anyhow::Result<Vec<usize>> {
        self.columns
            .iter()
            .map(|col| match col {
                KeyColumn::Index(index) if *index < fields.len() => Ok(*index),
                KeyColumn::Index(index) => Err(anyhow::anyhow!("Column index {index} out of range")),
                KeyColumn::Name(name) => fields
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| anyhow::anyhow!("Column '{name}' not found")),
            })
            .collect()
    }
}
