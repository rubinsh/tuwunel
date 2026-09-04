use tuwunel_core::{Result, err};

use super::parse_alias_from_localpart;
use crate::admin_command;

#[admin_command]
pub(super) async fn alias_remove(&self, room_alias_localpart: String) -> Result {
	let room_alias = parse_alias_from_localpart(self.services, &room_alias_localpart)?;

	let id = self
		.services
		.alias
		.resolve_local_alias(&room_alias)
		.await
		.map_err(|_| err!("Alias isn't in use."))?;

	self.services
		.alias
		.remove_alias(&room_alias)
		.await
		.map_err(|err| err!("Failed to remove alias: {err}"))?;

	write!(self, "Removed alias from {id}").await
}
