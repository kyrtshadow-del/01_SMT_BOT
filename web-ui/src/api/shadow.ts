import { api } from "./client";

export interface UnknownDevice {
  protocol: string;
  uid: string;
  last_seen_ts: number;
  last_ip: string | null;
  params_seen: string[];
  lat?: number | null;
  lon?: number | null;
}

export const getUnknownDevices = async (): Promise<UnknownDevice[]> => {
  const { data } = await api.get<UnknownDevice[]>("/api/unknown_devices");
  return data;
};

export interface CreateAndBindResponse {
  status: string;
  unit_id?: number;
  device_id?: number;
}

export const createUnitFromShadow = async (
  protocol: string,
  uid: string,
  name: string
): Promise<CreateAndBindResponse> => {
  const { data } = await api.post<CreateAndBindResponse>(
    `/api/unknown_devices/${encodeURIComponent(protocol)}/${encodeURIComponent(
      uid
    )}/create`,
    { name, priority: 0 }
  );
  return data;
};

export const createUnitsBatch = async (
  devices: UnknownDevice[],
  onProgress: (done: number) => void
): Promise<void> => {
  const CHUNK_SIZE = 20;
  let completed = 0;

  for (let i = 0; i < devices.length; i += CHUNK_SIZE) {
    const chunk = devices.slice(i, i + CHUNK_SIZE);

    await Promise.all(
      chunk.map((d) =>
        api
          .post(
            `/api/unknown_devices/${encodeURIComponent(
              d.protocol
            )}/${encodeURIComponent(d.uid)}/create`,
            {
              name: `${d.protocol.toUpperCase()} ${d.uid.slice(-6)}`,
              priority: 0,
            }
          )
          .catch((e) => {
            // не валим весь импорт из-за одной ошибки
            // eslint-disable-next-line no-console
            console.warn(`Failed to import ${d.uid}`, e);
          })
      )
    );

    completed += chunk.length;
    onProgress(completed);
  }
};

export const ignoreShadowDevice = async (
  protocol: string,
  uid: string
): Promise<void> => {
  await api.post(
    `/api/unknown_devices/${encodeURIComponent(
      protocol
    )}/${encodeURIComponent(uid)}/ignore`,
    {}
  );
};

