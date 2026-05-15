use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SingleOrderLocation {
    Single,
    OneCancelsOther(OneCancelsOtherLocation),
    OneTriggersOther(OneTriggersOtherLocation),
    OneTriggersOco(OneTriggersOcoLocation),
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OneCancelsOtherLocation {
    Primary,
    Secondary,
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OneTriggersOtherLocation {
    Primary,
    Secondary,
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OneTriggersOcoLocation {
    Primary,
    OcoPrimary,
    OcoSecondary,
}
