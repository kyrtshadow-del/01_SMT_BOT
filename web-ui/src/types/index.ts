export interface Unit {
  id: number;
  name: string;
  lat: number | null;
  lon: number | null;
  course: number;
  speed: number | null;
  online: boolean;
  icon_kind?: string | null;
  status:
    | "moving"
    | "stopped"
    | "stop"
    | "park_ign_on"
    | "park_ign_off"
    | "offline";
  status_label: string;
  last_ts: number;
}

export interface AuthState {
  sessionId: string | null;
  isAuthenticated: boolean;
  login: (sid: string) => void;
  logout: () => void;
}
