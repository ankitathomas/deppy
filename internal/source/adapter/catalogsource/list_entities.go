package catalogsource

import (
	"github.com/operator-framework/deppy/internal/lib/grpc"
	"github.com/operator-framework/deppy/internal/lib/util"
	"github.com/operator-framework/deppy/internal/source"
	"github.com/operator-framework/operator-registry/alpha/property"
	catalogsourceapi "github.com/operator-framework/operator-registry/pkg/api"

	"context"
	"fmt"
	"io"
)

type upgradeEdge struct {
	Replaces  string   `json:"replaces,omitempty"`
	Skips     []string `json:"skips,omitempty"`
	SkipRange string   `json:"skipRange,omitempty"`
}

// experimental type to represent union of upgrade edges
const TypeUpgrade string = "olm.upgradeFrom"

// ListEntities implementation
func (s *CatalogSourceDeppyAdapter) listEntities(ctx context.Context) ([]*source.Entity, error) {
	// TODO: sync catsrc state separately
	if s.catsrc.Namespace != "" && s.catsrc.Name != "" {
		catsrc, err := s.catsrcLister.CatalogSources(s.catsrc.Namespace).Get(s.catsrc.Name)
		if err != nil {
			return nil, err
		}
		s.catsrc = catsrc
	}

	conn, err := grpc.ConnectWithTimeout(ctx, s.catsrc.Address(), s.catsrcTimeout)
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return nil, err
	}

	catsrcClient := catalogsourceapi.NewRegistryClient(conn)
	stream, err := catsrcClient.ListBundles(ctx, &catalogsourceapi.ListBundlesRequest{})
	// not obtained through ListBundles:
	//type Bundle struct {
	//	CsvJson      string              `protobuf:"bytes,4,opt,name=csvJson,proto3" json:"csvJson,omitempty"`
	//	Object       []string            `protobuf:"bytes,5,rep,name=object,proto3" json:"object,omitempty"`
	//}
	// If needed in resolution, will need to add a GetBundle call

	if err != nil {
		return nil, fmt.Errorf("ListBundles failed: %v", err)
	}

	var entities []*source.Entity

	for {
		bundle, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return entities, fmt.Errorf("failed to read bundle stream: %v\n", err)
		}

		entities = append(entities, entityFromBundle(s.catsrc.Name, bundle))
	}
	return entities, nil
}

func entityFromBundle(catsrcName string, bundle *catalogsourceapi.Bundle) *source.Entity {
	return source.NewEntity(idFromBundle(catsrcName, bundle), propertiesFromBundle(catsrcName, bundle))
}

func idFromBundle(catsrcId string, bundle *catalogsourceapi.Bundle) source.EntityID {
	//bundlePath??
	return source.EntityID{
		CSVName: bundle.CsvName,
		Version: bundle.Version,
		Package: bundle.PackageName,
		Source:  catsrcId,
	}
}

func propertiesFromBundle(catsrcName string, bundle *catalogsourceapi.Bundle) map[string]string {
	properties := map[string]string{}
	errors := []error{}

	propsList := map[string]map[string]struct{}{
		property.TypeGVK:         {},
		property.TypeGVKRequired: {},
		property.TypePackage:     {},
		property.TypeChannel:     {},
		TypeUpgrade:              {},
	}

	for _, prvApi := range bundle.ProvidedApis {
		apiValue, err := util.JSONMarshal(prvApi)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		propsList[property.TypeGVK][string(apiValue)] = struct{}{}
	}

	for _, reqApi := range bundle.RequiredApis {
		apiValue, err := util.JSONMarshal(reqApi)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		propsList[property.TypeGVKRequired][string(apiValue)] = struct{}{}
	}

	for _, reqApi := range bundle.Dependencies {
		propsList[property.TypeGVKRequired][reqApi.Value] = struct{}{}
	}

	pkgValue, err := util.JSONMarshal(property.Package{
		PackageName: bundle.PackageName,
		Version:     bundle.Version,
	})
	if err != nil {
		errors = append(errors, err)
	} else {
		propsList[property.TypePackage][string(pkgValue)] = struct{}{}
	}

	chValue, err := util.JSONMarshal(property.Channel{
		ChannelName: bundle.ChannelName,
	})
	if err != nil {
		errors = append(errors, err)
	} else {
		propsList[property.TypeChannel][string(chValue)] = struct{}{}
	}

	if len(bundle.Replaces) != 0 || len(bundle.Skips) != 0 || len(bundle.SkipRange) != 0 {
		upValue, err := util.JSONMarshal(upgradeEdge{
			Replaces:  bundle.Replaces,
			Skips:     bundle.Skips,
			SkipRange: bundle.SkipRange,
		})
		if err != nil {
			errors = append(errors, err)
		} else {
			propsList[TypeUpgrade][string(upValue)] = struct{}{}
		}
	}

	for _, p := range bundle.Properties {
		if _, ok := propsList[p.Type]; !ok {
			propsList[p.Type] = map[string]struct{}{}
		}
		propsList[p.Type][p.Value] = struct{}{}
	}

	for pType, pValues := range propsList {
		props := []property.Property{}
		for pValue := range pValues {
			props = append(props, property.Property{
				Type:  pType,
				Value: []byte(pValue),
			})
		}
		if len(props) == 0 {
			continue
		}
		pValue, err := util.JSONMarshal(props)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		properties[pType] = string(pValue)
	}
	return properties
}