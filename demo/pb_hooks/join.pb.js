// POST /join picks the oldest room with space, or makes a new one, and returns its id.
routerAdd("POST", "/join", (e) => {
  const max = $app.store().get("pocketsocket:max");
  for (const room of e.app.findRecordsByFilter("rooms", "", "created", 100, 0)) {
    const seats = $app.store().get("pocketsocket:rooms/" + room.id + "/state") || 0;
    if (!max || seats < max) return e.json(200, { room: room.id });
  }
  const room = new Record(e.app.findCollectionByNameOrId("rooms"));
  e.app.save(room);
  return e.json(200, { room: room.id });
});
