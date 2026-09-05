export interface ChatMessage {
  id: string;
  author: string;
  text: string;
  createdAt: number;
}

export interface ForceTrack {
  id: string;
  callsign: string;
  affiliation: "friendly" | "unknown" | "hostile";
  latitude: number;
  longitude: number;
  updatedAt: number;
  updatedBy: string;
}

export interface Point {
  x: number;
  y: number;
}

export interface Drawing {
  id: string;
  author: string;
  color: string;
  points: Point[];
  createdAt: number;
}

export interface FieldDocument extends Record<string, unknown> {
  schemaVersion: 1;
  messages: ChatMessage[];
  tracks: Record<string, ForceTrack>;
  drawings: Drawing[];
}

export type ClientCommand =
  | { type: "chat.add"; text: string }
  | { type: "track.upsert"; id?: string; callsign: string; affiliation: ForceTrack["affiliation"]; latitude: number; longitude: number }
  | { type: "drawing.add"; color: string; points: Point[] }
  | { type: "drawing.clear" };
