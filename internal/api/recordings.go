package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"orbitlink/internal/audit"
	"orbitlink/internal/decoder"
	"orbitlink/internal/recorder"
)

func (r *Runtime) listRecordings(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.Recorder.List()})
}

func (r *Runtime) appendFrame(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Sequence uint64 `json:"sequence"`
		Epoch    int64  `json:"epoch"`
		Size     int    `json:"size"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	if input.Size == 0 {
		input.Size = 64
	}
	payload := decoder.EncodeTelemetry(input.Sequence, input.Size)
	err := r.Recorder.Buffer(chi.URLParam(request, "segmentID"), recorder.Frame{Sequence: input.Sequence, Epoch: input.Epoch, Received: time.Now().UTC(), Payload: payload})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"buffered": true, "sequence": input.Sequence})
}

func (r *Runtime) listAudit(writer http.ResponseWriter, request *http.Request) {
	values := r.Audit.List(audit.Query{Component: request.URL.Query().Get("component"), Subject: request.URL.Query().Get("subject"), Limit: 100})
	writeJSON(writer, http.StatusOK, map[string]any{"items": values})
}

func (r *Runtime) listPersistentEvents(writer http.ResponseWriter, request *http.Request) {
	values, err := r.EventLog.Read(chi.URLParam(request, "stream"), 100)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": values})
}

func (r *Runtime) prepareHandover(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Destination string `json:"destination"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.Handovers.Prepare(chi.URLParam(request, "passID"), input.Destination)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, session)
}

func (r *Runtime) confirmHandover(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Station string `json:"station"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.Handovers.Confirm(chi.URLParam(request, "sessionID"), input.Station)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (r *Runtime) commitHandover(writer http.ResponseWriter, request *http.Request) {
	session, err := r.Handovers.Commit(chi.URLParam(request, "sessionID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (r *Runtime) abortHandover(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Reason string `json:"reason"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.Handovers.Abort(chi.URLParam(request, "sessionID"), input.Reason)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (r *Runtime) listHandovers(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.Handovers.List(chi.URLParam(request, "passID"))})
}

func (r *Runtime) lockReceiver(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		AttemptID string `json:"attempt_id"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.receiverSession(chi.URLParam(request, "passID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	epoch, err := session.OnLock(input.AttemptID)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"epoch": epoch})
}

func (r *Runtime) loseCarrier(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		AttemptID string `json:"attempt_id"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.receiverSession(chi.URLParam(request, "passID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	if err := session.OnCarrierLoss(input.AttemptID); err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"carrier_lost": true})
}

func (r *Runtime) feedSymbols(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		AttemptID string `json:"attempt_id"`
		Symbols   []byte `json:"symbols"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.receiverSession(chi.URLParam(request, "passID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	frames, err := session.Feed(input.AttemptID, input.Symbols)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"frames_buffered": frames})
}

func (r *Runtime) listClockCorrections(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.Clock.Corrections()})
}
